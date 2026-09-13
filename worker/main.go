package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type Event struct {
	ID         string                 `json:"id"`
	EndpointID string                 `json:"endpoint_id"`
	Headers    map[string][]string    `json:"headers"`
	Payload    map[string]interface{} `json:"payload"`
	Status     string                 `json:"status"`
	ReceivedAt time.Time              `json:"received_at"`
}

func main() {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		log.Fatal("KAFKA_BROKER environment variable is required")
	}

	ctx := context.Background()

	// Create database connection pool
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer pool.Close()

	// Initialize Kafka Reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		GroupID:  "worker-group",
		Topic:    "webhook.events",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer reader.Close()

	// Initialize Kafka Writer for DLQ
	dlqWriter := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    "webhook.dlq",
		Balancer: &kafka.LeastBytes{},
	}
	defer dlqWriter.Close()

	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}

	log.Println("Worker started, waiting for messages...")

	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message: %v\n", err)
			continue
		}

		var event Event
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("Failed to unmarshal event: %v", err)
			continue
		}

		log.Printf("Processing event %s for endpoint %s", event.ID, event.EndpointID)

		// 1. Look up destination URL
		var destURL string
		err = pool.QueryRow(ctx, "SELECT url FROM endpoints WHERE id=$1", event.EndpointID).Scan(&destURL)
		if err != nil {
			log.Printf("Failed to get URL for endpoint %s: %v", event.EndpointID, err)
			updateStatus(ctx, pool, event.ID, "failed")
			continue
		}

		// 2. Perform HTTP POST with Retries
		maxRetries := 5
		delivered := false
		backoff := 1 * time.Second

		for attempt := 1; attempt <= maxRetries; attempt++ {
			if attempt > 1 {
				log.Printf("Retry attempt %d for event %s. Waiting %v...", attempt, event.ID, backoff)
				time.Sleep(backoff)
				backoff *= 2 // exponential backoff
			}

			payloadBytes, _ := json.Marshal(event.Payload)
			req, err := http.NewRequest("POST", destURL, bytes.NewBuffer(payloadBytes))
			if err != nil {
				log.Printf("Failed to create request: %v", err)
				continue
			}

			req.Header.Set("Content-Type", "application/json")
			// Add original headers
			for k, values := range event.Headers {
				for _, v := range values {
					req.Header.Add(k, v)
				}
			}

			resp, err := httpClient.Do(req)
			if err != nil {
				log.Printf("Request failed: %v", err)
				continue
			}
			
			resp.Body.Close()

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				delivered = true
				break
			} else {
				log.Printf("Received non-2xx status code: %d", resp.StatusCode)
			}
		}

		// 3. Handle success or failure
		if delivered {
			log.Printf("Successfully delivered event %s", event.ID)
			updateStatus(ctx, pool, event.ID, "delivered")
		} else {
			log.Printf("Failed to deliver event %s after %d attempts", event.ID, maxRetries)
			updateStatus(ctx, pool, event.ID, "dead_letter")
			
			// Publish to DLQ
			err = dlqWriter.WriteMessages(ctx, kafka.Message{
				Key:   m.Key,
				Value: m.Value,
			})
			if err != nil {
				log.Printf("Failed to publish to DLQ: %v", err)
			} else {
				log.Printf("Published event %s to DLQ", event.ID)
			}
		}
	}
}

func updateStatus(ctx context.Context, pool *pgxpool.Pool, eventID string, status string) {
	_, err := pool.Exec(ctx, "UPDATE events SET status=$1 WHERE id=$2", status, eventID)
	if err != nil {
		log.Printf("Failed to update status for event %s to %s: %v", eventID, status, err)
	}
}
