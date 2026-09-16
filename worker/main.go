package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// ─── Prometheus Metrics ───────────────────────────────────────────────────────

var (
	eventsProcessedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "worker_events_processed_total",
		Help: "Total events processed, labelled by outcome status (delivered, dead_letter, failed).",
	}, []string{"status"})

	retryAttemptsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "worker_retry_attempts_total",
		Help: "Total number of delivery retry attempts made by the worker.",
	})

	deliveryDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "worker_delivery_duration_seconds",
		Help:    "Time taken to deliver an event to its destination URL (all attempts combined).",
		Buckets: prometheus.DefBuckets,
	})
)

// ─── Types ────────────────────────────────────────────────────────────────────

type Event struct {
	ID         string                 `json:"id"`
	EndpointID string                 `json:"endpoint_id"`
	Headers    map[string][]string    `json:"headers"`
	Payload    map[string]interface{} `json:"payload"`
	Status     string                 `json:"status"`
	ReceivedAt time.Time              `json:"received_at"`
}

// kafkaHeaderCarrier adapts Kafka message headers to satisfy the OTel
// TextMapCarrier interface so trace context can be extracted from consumed messages.
type kafkaHeaderCarrier struct {
	headers []kafka.Header
}

func (c kafkaHeaderCarrier) Get(key string) string {
	for _, h := range c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *kafkaHeaderCarrier) Set(key, val string) {
	c.headers = append(c.headers, kafka.Header{Key: key, Value: []byte(val)})
}

func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, len(c.headers))
	for i, h := range c.headers {
		keys[i] = h.Key
	}
	return keys
}

// ─── OpenTelemetry Setup ──────────────────────────────────────────────────────

// initTracer configures the global OTel TracerProvider to export to the given
// OTLP gRPC endpoint (e.g., Jaeger). Returns a shutdown function.
func initTracer(ctx context.Context, serviceName, otlpEndpoint string) (func(context.Context) error, error) {
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otlpEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTel resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	log.Printf("OTel tracing initialized → %s", otlpEndpoint)
	return tp.Shutdown, nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func updateStatus(ctx context.Context, pool *pgxpool.Pool, eventID string, status string) {
	_, err := pool.Exec(ctx, "UPDATE events SET status=$1 WHERE id=$2", status, eventID)
	if err != nil {
		log.Printf("Failed to update status for event %s to %s: %v", eventID, status, err)
	}
}

// ─── Main ─────────────────────────────────────────────────────────────────────

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

	// ── OTel Tracing (optional — skipped gracefully if not configured) ──
	if otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); otlpEndpoint != "" {
		shutdown, err := initTracer(ctx, "worker", otlpEndpoint)
		if err != nil {
			log.Printf("Warning: could not init OTel tracer: %v", err)
		} else {
			defer shutdown(ctx)
		}
	}

	// ── Metrics HTTP server on a separate port (for Prometheus scraping) ──
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		log.Println("Worker metrics server listening on :9091")
		if err := http.ListenAndServe(":9091", mux); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	// ── Database ──
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer pool.Close()

	// ── Kafka Reader ──
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		GroupID:  "worker-group",
		Topic:    "webhook.events",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	// ── Kafka DLQ Writer ──
	dlqWriter := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    "webhook.dlq",
		Balancer: &kafka.LeastBytes{},
	}
	defer dlqWriter.Close()

	httpClient := &http.Client{Timeout: 10 * time.Second}

	log.Println("Worker started, waiting for messages...")

	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message: %v\n", err)
			continue
		}

		// Extract OTel trace context from Kafka message headers.
		// This links this span as a child of the ingestion-api's span in Jaeger.
		carrier := kafkaHeaderCarrier{headers: m.Headers}
		msgCtx := otel.GetTextMapPropagator().Extract(context.Background(), &carrier)

		tracer := otel.Tracer("worker")
		msgCtx, span := tracer.Start(msgCtx, "process_event")

		var event Event
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("Failed to unmarshal event: %v", err)
			span.End()
			continue
		}

		span.SetAttributes(
			attribute.String("event.id", event.ID),
			attribute.String("endpoint.id", event.EndpointID),
		)
		log.Printf("Processing event %s for endpoint %s", event.ID, event.EndpointID)

		// 1. Look up destination URL
		var destURL string
		err = pool.QueryRow(msgCtx, "SELECT url FROM endpoints WHERE id=$1", event.EndpointID).Scan(&destURL)
		if err != nil {
			log.Printf("Failed to get URL for endpoint %s: %v", event.EndpointID, err)
			updateStatus(msgCtx, pool, event.ID, "failed")
			eventsProcessedTotal.WithLabelValues("failed").Inc()
			span.End()
			continue
		}

		// 2. Deliver with retries + exponential backoff
		maxRetries := 5
		delivered := false
		backoff := 1 * time.Second
		deliveryStart := time.Now()

		for attempt := 1; attempt <= maxRetries; attempt++ {
			if attempt > 1 {
				log.Printf("Retry attempt %d for event %s. Waiting %v...", attempt, event.ID, backoff)
				time.Sleep(backoff)
				backoff *= 2
				retryAttemptsTotal.Inc()
			}

			payloadBytes, _ := json.Marshal(event.Payload)
			req, err := http.NewRequest("POST", destURL, bytes.NewBuffer(payloadBytes))
			if err != nil {
				log.Printf("Failed to create request: %v", err)
				continue
			}
			req.Header.Set("Content-Type", "application/json")
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
			}
			log.Printf("Non-2xx status code: %d", resp.StatusCode)
		}

		// 3. Record result
		deliveryDuration.Observe(time.Since(deliveryStart).Seconds())

		if delivered {
			log.Printf("Successfully delivered event %s", event.ID)
			updateStatus(msgCtx, pool, event.ID, "delivered")
			eventsProcessedTotal.WithLabelValues("delivered").Inc()
		} else {
			log.Printf("Failed to deliver event %s after %d attempts", event.ID, maxRetries)
			updateStatus(msgCtx, pool, event.ID, "dead_letter")
			eventsProcessedTotal.WithLabelValues("dead_letter").Inc()

			if err := dlqWriter.WriteMessages(ctx, kafka.Message{
				Key:   m.Key,
				Value: m.Value,
			}); err != nil {
				log.Printf("Failed to publish to DLQ: %v", err)
			} else {
				log.Printf("Published event %s to DLQ", event.ID)
			}
		}

		span.End()
	}
}
