# Build stage — copies everything first so go mod tidy can resolve all imports
FROM golang:latest AS builder
WORKDIR /app
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o /worker main.go

# Run stage — minimal Alpine image
FROM alpine:latest
WORKDIR /root/
COPY --from=builder /worker .
EXPOSE 9091
CMD ["./worker"]
