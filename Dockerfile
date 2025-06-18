# Use the official Golang image
FROM golang:1.23

# Set the working directory
WORKDIR /app

# Copy go.mod and go.sum
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o matching-engine cmd/api/main.go

# Expose ports
EXPOSE 3000 8080

# Run the application
CMD ["./matching-engine"]