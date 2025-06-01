package grpc

import (
	"context"
	"github.com/google/uuid"
	"github.com/upekZ/matching-engine/internal/engine"
	"github.com/upekZ/matching-engine/internal/models"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
)

// OrderServiceServer defines the gRPC service.
type OrderServiceServer struct {
	engine *engine.Engine
	UnimplementedOrderServiceServer
}

// PlaceOrder handles incoming order requests.
func (s *OrderServiceServer) PlaceOrder(ctx context.Context, req *OrderRequest) (*OrderResponse, error) {
	// Validate order
	if req.Price <= 0 || req.Quantity <= 0 || (req.Side != "buy" && req.Side != "sell") {
		return nil, status.Error(codes.InvalidArgument, "Invalid order parameters")
	}

	// Create order
	order := models.NewOrder(uuid.New().String(), req.Market, req.Side, req.Price, req.Quantity, time.Now().UnixNano())

	// Forward to engine
	trade, err := s.engine.PlaceOrder(order)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to process order")
	}

	// Prepare response
	resp := &OrderResponse{
		OrderID: order.ID,
		Status:  "open",
	}
	if trade != nil {
		resp.Status = "filled"
		resp.Trade = &Trade{
			ID:        trade.ID,
			Market:    trade.market,
			Price:     trade.price,
			Quantity:  trade.quantity,
			BuyOrder:  trade.buyOrder,
			SellOrder: trade.sellOrder,
			Timestamp: trade.timestamp,
		}
	}

	return resp, nil
}

// NewServer initializes the gRPC server.
func NewServer(port string, eng *engine.Engine) (*grpc.Server, error) {
	srv := grpc.NewServer()
	RegisterOrderServiceServer(srv, &OrderServiceServer{engine: eng})
	go func() {
		lis, err := net.Listen("tcp", ":"+port)
		if err != nil {
			log.Fatalf("Failed to listen: %v", err)
		}
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()
	return srv, nil
}
