# Matching Engine

A Matching Engine for trading systems, built with Go. It supports order placement via REST APIs and real-time trade updates via gRPC streaming for internal gateways. This project can be run either locally or using Docker for a containerized environment.

---

## Features

- **REST API**: Place buy/sell orders (`POST /orders`) and cancel orders (`DELETE /orders`) with fields like `client_id`, `symbol`, `side`, `price`, and `quantity`.
- **gRPC Streaming**: Subscribe to trade updates for specific markets via `SubscribeOrderUpdates` (port `50051`).
- **High-Level Architecture**:
    - **Presentation**:
        - REST (`internal/api/rest`) on port `3000`
        - gRPC (`internal/api/grpc`) on port `50051`
    - **Business Logic**: Matching engine (`internal/engine`) with channel-based order books per symbol.
    - **Data**: Redis (`internal/storage/redisCache`) for persistence and Pub/Sub.
- **Order Types**: Supports limit orders; future expansion planned for market and other order types.
- **Testing**: Includes unit tests (`internal/engine`) and integration tests (`tests/integration_test.go`).

---

## Prerequisites

### Local Setup
- **Go**: Version `1.23` or later.
- **Redis**: Version `6.0` or later, running on `localhost:6379`.
- **PostgreSQL**: Version `13` or later, running on `localhost:5432`.
- **Tools**:
    - Postman (>=9.0) for testing REST and gRPC endpoints
    - `protoc` for gRPC code generation
- **Dependencies**:
```bash
go get github.com/google/uuid@v1.6.0
go get github.com/gorilla/mux@v1.8.1
go get github.com/gorilla/websocket@v1.5.3
go get github.com/redis/go-redis/v9@v9.6.1
go get google.golang.org/grpc@v1.67.0
go get google.golang.org/protobuf@v1.34.2
```

### Docker Setup
- **Docker**: Version `20.10` or later.
- **Docker Compose**: Version `2.17.0` or later (plugin-based).
- **Go**: Version `1.23` or later (included in Docker image).
- **No Local Redis or PostgreSQL**: Dockerized instances are provided.

---

## Setup

### Local Setup
```bash
# Clone the Repository
git clone https://github.com/upekZ/matching-engine.git
cd matching-engine

# Install Dependencies
go mod tidy

# Start Redis and PostgreSQL
redis-server
redis-cli ping  # Should return PONG
sudo service postgresql start  # Or your PostgreSQL start command

# Generate gRPC Code
protoc --go_out=. --go-grpc_out=. internal/api/grpc/order_service.proto

# Build and Run
cd cmd/api
go build ./api
```

### Docker Setup
```bash
# Clone the Repository
git clone https://github.com/upekZ/matching-engine.git
cd matching-engine

# Install Docker and Docker Compose
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-plugin

# Generate gRPC Code
protoc --go_out=. --go-grpc_out=. internal/api/grpc/order_service.proto

# Build and Run
docker compose up --build
```
- REST server runs on `:3000`, gRPC server on `:50051`.

---

## Usage

### Place/Cancel an Order (REST API)

- **Endpoint**: `POST /orders`, `DELETE /orders`
- **Example**:
```bash
curl -X POST http://localhost:3000/orders -H "Content-Type: application/json" -d '{
  "client_id": "client1",
  "symbol": "BTC-USD",
  "side": "1",
  "price": 50000.0,
  "quantity": 1,
  "type": "2"
}'
```
- **Response**: JSON with `order_id`, `status`, and `executions`.

### Subscribe to Execution Updates (gRPC)

- **RPC**: `SubscribeOrderUpdates`
- **Steps**:
    1. Use Postman (>=9.0)
    2. Create a new gRPC request
    3. Import `internal/api/grpc/order_service.proto`
    4. Set server URL: `localhost:50051` (insecure)
    5. Invoke `grpc.OrderService/SubscribeOrderUpdates` with:
```json
{
  "market": "BTC-USD"
}
```
6. Place orders via REST to trigger updates
7. Observe streamed `ExecReport` messages

---

## Architecture

### Presentation Layer
- REST API (`internal/api/rest`): Port `3000`
- gRPC (`internal/api/grpc`): Port `50051`, streams trade updates

### Business Logic
- Engine (`internal/engine`): Processes orders with goroutines per symbol using `orderBook`

### Data Layer
- Redis (`internal/storage/redisCache`): Stores executions (`trade:<id>`) and Pub/Sub (`order_responses:<symbol>`)
- PostgreSQL: For persistent storage (via `init.sql`)
- Type Safety: Uses `models.OrderResponse` to decouple gRPC from business logic

---

## Future Improvements

- Modify `orderBook` to use order copies before updates
- Expand to market and other order types
- Add modify order functionality
- Support cancel by client ID without order type
- Separate Redis for store and broker
- Enhance integration and unit tests
- Add persistent storage backups
- Implement WebSocket with gRPC

---

## Running with Makefile

### Build
- Locally: `make build`
- Docker: `make USE_DOCKER=true build`

### Run
- Locally: `make run`
- Docker: `make USE_DOCKER=true run`

### Integration Tests
- Locally: `make int-test`
- Docker: `make USE_DOCKER=true int-test`

### Unit Tests
- Locally: `make unit-test`
- Docker: `make USE_DOCKER=true unit-test`

### Clean
- Locally: `make clean`
- Docker: `make USE_DOCKER=true clean`

### Setup
- `make setup` (works for both, installs deps and generates gRPC code)