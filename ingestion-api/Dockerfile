FROM golang:latest AS builder

WORKDIR /app

COPY go.mod ./
# Download dependencies based on go.mod
RUN go mod download

COPY . .
# Run go mod tidy to ensure everything is resolved, generating go.sum
RUN go mod tidy

RUN CGO_ENABLED=0 GOOS=linux go build -o /api main.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /api .

EXPOSE 8080
CMD ["./api"]
