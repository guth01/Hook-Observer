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
	webhookRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_requests_total",
		Help: "Total number of incoming webhook requests, labelled by endpoint_id and status.",
	}, []string{"endpoint_id", "status"})

	webhookRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "webhook_request_duration_seconds",
		Help:    "End-to-end latency of webhook ingestion (receive → Kafka publish).",
		Buckets: prometheus.DefBuckets,
	}, []string{"endpoint_id"})

	kafkaPublishErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kafka_publish_errors_total",
		Help: "Total number of failed Kafka publish attempts.",
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

// kafkaHeaderCarrier adapts a Kafka message header slice to satisfy the OTel
// TextMapCarrier interface so trace context can be injected into Kafka messages.
type kafkaHeaderCarrier []kafka.Header

func (c *kafkaHeaderCarrier) Get(key string) string {
	for _, h := range *c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *kafkaHeaderCarrier) Set(key, val string) {
	*c = append(*c, kafka.Header{Key: key, Value: []byte(val)})
}

func (c *kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, len(*c))
	for i, h := range *c {
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
		shutdown, err := initTracer(ctx, "ingestion-api", otlpEndpoint)
		if err != nil {
			log.Printf("Warning: could not init OTel tracer: %v", err)
		} else {
			defer shutdown(ctx)
		}
	}

	// ── Database ──
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer pool.Close()

	// ── Kafka Writer ──
	kafkaWriter := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    "webhook.events",
		Balancer: &kafka.LeastBytes{},
	}
	defer kafkaWriter.Close()

	// ── HTTP Router ──
	r := chi.NewRouter()

	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	// Expose Prometheus metrics at /metrics
	r.Handle("/metrics", promhttp.Handler())

	// ── POST /webhook/{endpointId} ──
	r.Post("/webhook/{endpointId}", func(w http.ResponseWriter, r *http.Request) {
		endpointID := chi.URLParam(r, "endpointId")
		start := time.Now()

		// Start OTel span for this request
		tracer := otel.Tracer("ingestion-api")
		spanCtx, span := tracer.Start(r.Context(), "receive_webhook")
		span.SetAttributes(attribute.String("endpoint.id", endpointID))
		defer span.End()

		// 1. Verify endpoint exists in DB
		var exists bool
		err := pool.QueryRow(spanCtx, "SELECT EXISTS(SELECT 1 FROM endpoints WHERE id=$1)", endpointID).Scan(&exists)
		if err != nil {
			http.Error(w, "Database error", http.StatusInternalServerError)
			webhookRequestsTotal.WithLabelValues(endpointID, "db_error").Inc()
			return
		}
		if !exists {
			http.Error(w, "Endpoint not found (try using 'test-endpoint')", http.StatusNotFound)
			webhookRequestsTotal.WithLabelValues(endpointID, "not_found").Inc()
			return
		}

		// 2. Extract request headers
		headers := r.Header

		// 3. Read raw request body
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading body", http.StatusInternalServerError)
			webhookRequestsTotal.WithLabelValues(endpointID, "read_error").Inc()
			return
		}

		// 4. Parse JSON body (fallback to raw string)
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
		_, err = pool.Exec(spanCtx,
			"INSERT INTO events (id, endpoint_id, headers, payload, status) VALUES ($1, $2, $3, $4, $5)",
			eventID, endpointID, headers, payload, "pending",
		)
		if err != nil {
			log.Printf("Failed to insert event: %v", err)
			http.Error(w, "Database error", http.StatusInternalServerError)
			webhookRequestsTotal.WithLabelValues(endpointID, "db_error").Inc()
			return
		}

		// 6. Build the Kafka message payload
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

		// Inject OTel trace context into Kafka message headers for end-to-end tracing
		carrier := kafkaHeaderCarrier{}
		otel.GetTextMapPropagator().Inject(spanCtx, &carrier)

		// 7. Publish to Kafka
		err = kafkaWriter.WriteMessages(r.Context(),
			kafka.Message{
				Key:     []byte(endpointID),
				Value:   msgBytes,
				Headers: []kafka.Header(carrier),
			},
		)
		if err != nil {
			log.Printf("Failed to publish event to Kafka: %v", err)
			kafkaPublishErrorsTotal.Inc()
			http.Error(w, "Queue error", http.StatusInternalServerError)
			webhookRequestsTotal.WithLabelValues(endpointID, "kafka_error").Inc()
			return
		}

		// Record success metrics
		webhookRequestsTotal.WithLabelValues(endpointID, "accepted").Inc()
		webhookRequestDuration.WithLabelValues(endpointID).Observe(time.Since(start).Seconds())

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(fmt.Sprintf(`{"status":"accepted","id":"%s"}`, eventID)))
	})

	// ── GET /events/{endpointId} ──
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
			if err := rows.Scan(&e.ID, &e.EndpointID, &e.Headers, &e.Payload, &e.Status, &e.ReceivedAt); err != nil {
				log.Printf("Row scan error: %v", err)
				continue
			}
			events = append(events, e)
		}

		if events == nil {
			events = []Event{}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(events)
	})

	fmt.Println("Ingestion API listening on :8080 (metrics at /metrics)")
	http.ListenAndServe(":8080", r)
}
