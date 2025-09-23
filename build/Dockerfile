# Build stage
FROM golang:tip-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build the Go application
RUN go build -o /app/bin/go-db-etl /app/cmd/main.go

# Final stage
FROM alpine:latest

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/bin/go-db-etl /app/bin/go-db-etl
COPY .env .env

# Optionally, copy static files if needed
# COPY --from=builder /app/static ./static

# Run the application
CMD ["/app/bin/go-db-etl"]