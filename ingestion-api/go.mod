module hook-observer/ingestion-api

go 1.23

require (
	github.com/go-chi/chi/v5 v5.0.12
	github.com/go-chi/cors v1.2.2
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.5.5
	github.com/prometheus/client_golang v1.19.1
	github.com/segmentio/kafka-go v0.4.51
	go.opentelemetry.io/otel v1.28.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.28.0
	go.opentelemetry.io/otel/sdk v1.28.0
)
