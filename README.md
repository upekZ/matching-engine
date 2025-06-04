# Matching Engine

A Matching engine for trading systems, built with Go. It supports order placement via REST APIs and real-time trade updates via gRPC streaming for internal gateways.

## Features
- **REST API**: Place buy/sell orders (`POST /orders, DELETE /orders`).
- **gRPC Streaming**: Subscribe to trade updates for specific markets (`SubscribeOrderUpdates`).
- **High-Level Architecture**:
    - **Presentation**: REST (`internal/api/rest`) and gRPC (`internal/api/grpc`).
    - **Business Logic**: Matching engine (`internal/engine`) with channel-based order books.
    - **Data**: Redis (`internal/storage/redis`) for persistence and Pub/Sub.
- **Redis Integration**: Stores trades and facilitates Pub/Sub for trade updates.

## Prerequisites
- **Go**: Version 1.22 or later.
- **Redis**: Version 6.0 or later, running on `localhost:6379`.
- **Postman**: For testing REST and gRPC endpoints.
- **Dependencies**:
  ```bash
  go get github.com/google/uuid@v1.6.0
  go get github.com/gorilla/mux@v1.8.1
  go get github.com/gorilla/websocket@v1.5.3
  go get github.com/redis/go-redis/v9@v9.6.1
  go get google.golang.org/grpc@v1.67.0
  go get google.golang.org/protobuf@v1.34.2
  ```

## Setup
1. **Clone the Repository**:
   ```bash
   git clone https://github.com/upekZ/matching-engine.git
   cd matching-engine
   ```

2. **Install Dependencies**:
   ```bash
   go mod tidy
   ```

3. **Start Redis**:
   Ensure Redis is running on `localhost:6379`:
   ```bash
   redis-server
   redis-cli ping  # Should return PONG
   ```

4. **Generate gRPC Code**:
   Compile the `order_service.proto` file:
   ```bash
   protoc --go_out=. --go-grpc_out=. internal/api/grpc/order_service.proto
   ```

5. **Build and Run**:
   ```bash
   cd cmd/api
   go build
   ./api
   ```
    - REST server runs on `:3000`.
    - gRPC server runs on `:8080`.

## Usage
### Place/Cancel an Order (REST API)
- **Endpoint**: `POST /orders`, `DELETE /orders`
- **Example**:
  ```bash
  curl -X POST http://localhost:8082/orders -H "Content-Type: application/json" -d '{
      "symbol": "BTC-USD",
      "client_id": "id-from-client"
      "side": "buy",
      "price": 50000.0,
      "quantity": 1,
      "req_type": "newLimitOrder"
  }'
  ```
    - REST response: created order with updated id and timestamp
    - Response to grpc subscribers: `{"order_id":"...", "status":"open", "trades":[]}`

### Subscribe to Trade Updates (gRPC)
- **RPC**: `SubscribeOrderUpdates`
- **Steps**:
    1. Use Postman (version ≥9.0):
        - Create a new gRPC request.
        - Import `internal/api/grpc/order_service.proto`.
        - Create a new API (e.g., `MatchingEngineAPI`).
        - Select `grpc.OrderService.SubscribeOrderUpdates`.
        - Set server URL: `localhost:8080` (insecure).
        - Set `MarketRequest`:
          ```json
          {
            "symbol": "BTC-USD"
          }
          ```
        - Click “Invoke” to start streaming.
    2. Place orders via REST to trigger updates (see above).
    3. Observe streamed `OrderResponse` messages in Postman:
        - Example: `{"order_id":"...", "trades":[{"id":"...", "symbol":"BTC-USD", ...}]}`

## Architecture
- **Presentation Layer**:
    - REST API (`internal/api/rest`): Handles order placement (New Order and Cancel Order).
    - gRPC (`internal/api/grpc`): Streams trade updates to gateways.
- **Business Logic**:
    - Engine (`internal/engine`): Processes orders using channels per symbol.
    - Processes multiple price levels concurrently
- **Data Layer**:
    - Redis (`internal/storage/redis`): Stores trades (`trade:<id>`) and facilitates Pub/Sub (`order_responses:<symbol>`).
- **Type Safety**:
    - Uses `models.OrderResponse` in `internal/models` to avoid gRPC dependencies in business logic.
    - gRPC types are isolated to the presentation layer.

## Future Improvements
- Modifications to orderBook are made when trades are executed. This will be updated to use copies of orders and then to alter orderbook once all trades are completed.
- Engine is only developed for LimitOrders. No Order types considered so far. to expand to other types of orders
- Current implementation expects to cancel->New when order needs to be modified. This needs to be expanded to Cancel and New if priority is impacted only and modify if no impact for priority --> Add Modify Order
- At the moment cancel order is defined to require an Order Type. This needs to be modified with ability to cancel by client ID
- Engine uses redis both as an in-memory store and a message broker. Implementation uses a single redis instance. functionality can be seperated to facilitate a separate store and broker
- Add Integration and Unit Tests
- Add a persistent storage for routine backups
- To Implement web-sockets with grpc

