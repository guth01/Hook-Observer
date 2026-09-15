package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"github.com/google/uuid"
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

	// Initialize Kafka Writer
	kafkaWriter := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    "webhook.events",
		Balancer: &kafka.LeastBytes{},
	}
	defer kafkaWriter.Close()


	// Initialize chi router
	r := chi.NewRouter()

	// Basic CORS
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	// POST /webhook/{endpointId}
	r.Post("/webhook/{endpointId}", func(w http.ResponseWriter, r *http.Request) {
		endpointID := chi.URLParam(r, "endpointId")

		// 1. Verify endpoint exists in DB
		var exists bool
		err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM endpoints WHERE id=$1)", endpointID).Scan(&exists)
		if err != nil {
			http.Error(w, "Database error", http.StatusInternalServerError)
			return
		}
		if !exists {
			http.Error(w, "Endpoint not found (try using 'test-endpoint')", http.StatusNotFound)
			return
		}

		// 2. Extract request headers
		headers := r.Header

		// 3. Read raw request body
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading body", http.StatusInternalServerError)
			return
		}
		
		// 4. Try parsing as JSON, fallback to raw string if not JSON
		var payload interface{}
		if len(bodyBytes) > 0 {
			if err := json.Unmarshal(bodyBytes, &payload); err != nil {
				payload = map[string]interface{}{"raw": string(bodyBytes)}
			}
		} else {
			payload = map[string]interface{}{}
		}

		eventID := uuid.New().String()
		
		// 5. Insert event into PostgreSQL
		_, err = pool.Exec(ctx,
			"INSERT INTO events (id, endpoint_id, headers, payload, status) VALUES ($1, $2, $3, $4, $5)",
			eventID, endpointID, headers, payload, "pending",
		)
		
		if err != nil {
			log.Printf("Failed to insert event: %v", err)
			http.Error(w, "Database error", http.StatusInternalServerError)
			return
		}

		// 6. Publish to Kafka
		var p map[string]interface{}
		if mapPayload, ok := payload.(map[string]interface{}); ok {
			p = mapPayload
		} else {
			p = map[string]interface{}{}
		}

		eventData := Event{
			ID:         eventID,
			EndpointID: endpointID,
			Headers:    headers,
			Payload:    p,
			Status:     "pending",
			ReceivedAt: time.Now(),
		}
		
		msgBytes, _ := json.Marshal(eventData)
		
		err = kafkaWriter.WriteMessages(r.Context(),
			kafka.Message{
				Key:   []byte(endpointID),
				Value: msgBytes,
			},
		)

		if err != nil {
			log.Printf("Failed to publish event to Kafka: %v", err)
			http.Error(w, "Queue error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted) // 202 Accepted
		w.Write([]byte(fmt.Sprintf(`{"status":"accepted", "id":"%s"}`, eventID)))
	})

	// GET /events/{endpointId}
	r.Get("/events/{endpointId}", func(w http.ResponseWriter, r *http.Request) {
		endpointID := chi.URLParam(r, "endpointId")

		rows, err := pool.Query(ctx, 
			"SELECT id, endpoint_id, headers, payload, status, received_at FROM events WHERE endpoint_id=$1 ORDER BY received_at DESC", 
			endpointID,
		)
		if err != nil {
			log.Printf("Query error: %v", err)
			http.Error(w, "Database error", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var events []Event
		for rows.Next() {
			var e Event
			err := rows.Scan(&e.ID, &e.EndpointID, &e.Headers, &e.Payload, &e.Status, &e.ReceivedAt)
			if err != nil {
				log.Printf("Row scan error: %v", err)
				continue
			}
			events = append(events, e)
		}

		if events == nil {
			events = []Event{} // Return empty array [] instead of null for JSON
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(events)
	})

	fmt.Println("Ingestion API listening on :8080")
	http.ListenAndServe(":8080", r)
}
