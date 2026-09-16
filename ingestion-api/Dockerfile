# Build stage — copies everything first so go mod tidy can resolve all imports
FROM golang:latest AS builder
WORKDIR /app
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o /api main.go

# Run stage — minimal Alpine image
FROM alpine:latest
WORKDIR /root/
COPY --from=builder /api .
EXPOSE 8080
CMD ["./api"]
