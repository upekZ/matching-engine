# Use the official Golang image as the base image
FROM golang:1.23 AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files to the workspace
COPY go.mod go.sum ./

# Download all dependencies
RUN go mod download

# Copy the source code into the container
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o matching-engine cmd/api/main.go
RUN ls -la  # Debug: List files to verify the binary

# Final stage: Create a lightweight image
FROM debian:bullseye-slim

# Install necessary runtime dependencies
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/matching-engine .
RUN ls -la  # Debug: List files in the final stage

# Copy any additional configuration files if needed (e.g., SQL init script)
COPY init.sql .

# Expose the ports (REST: 3000, gRPC: 8080)
EXPOSE 3000 8080

# Environment variables for database and Redis
ENV POSTGRES_CONN="postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable"
ENV REDIS_ADDR="redis:6379"

# Command to run the application
CMD ["./matching-engine"]