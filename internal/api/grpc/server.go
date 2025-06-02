package grpc

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/upekZ/matching-engine/internal/models"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"time"
)

type GlobalEngine interface {
	PlaceOrder(order *models.Order) (*models.TradeManager, error)
	CancelOrder(orderID string) error
	PublishOrderResponse(ctx context.Context, market string, data []byte) error
}

type OrderServiceHandler struct {
	engine GlobalEngine
	UnimplementedOrderServiceServer
}

func (s *OrderServiceHandler) PlaceOrder(ctx context.Context, req *OrderRequest) (*OrderResponse, error) {

	if req.Price <= 0 || req.Quantity <= 0 || (req.Side != "buy" && req.Side != "sell") {
		return nil, status.Error(codes.InvalidArgument, "Invalid order parameters")
	}

	order := models.NewOrder(uuid.New().String(), req.GetMarket(), req.GetSide(), req.GetPrice(), int(req.Quantity), time.Now().UnixNano())

	trades, err := s.engine.PlaceOrder(order)

	if err != nil {
		return nil, err
	}

	resp := &OrderResponse{
		OrderId: order.GetID(),
		Status:  "new",
	}
	if trades.GetVolume() > 0 {
		if order.GetQty() > trades.GetVolume() {
			resp.Status = "partially_filled"
		} else {
			resp.Status = "filled"
		}
		resp.Trades = make([]*Trade, len(trades.GetTrades()))
		for i, t := range trades.GetTrades() {
			resp.Trades[i] = &Trade{
				Id:        t.GetID(),
				Market:    order.GetMarket(),
				Price:     t.GetPrice(),
				Quantity:  int32(t.GetQty()),
				BuyOrder:  t.GetBuyOrderID(),
				SellOrder: t.GetSellOrderID(),
				Timestamp: t.GetTimestamp(),
			}
		}
	}

	data, err := json.Marshal(resp)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to serialize response")
	}
	if err := s.engine.PublishOrderResponse(ctx, req.Market, data); err != nil {
		log.Printf("Failed to publish data: %v", err)
		return nil, status.Error(codes.Internal, "Failed to forward response")
	}

	return resp, nil
}

func (s *OrderServiceHandler) CancelOrder(ctx context.Context, id string) error {

	err := s.engine.CancelOrder(id)

	if err != nil {
		log.Printf(status.Errorf(codes.Internal, "Failed to cancel order %v", err).Error())
		return status.Error(codes.Internal, "Failed to cancel order")
	}
}

func NewServer(port string, eng GlobalEngine) (*grpc.Server, error) {
	srv := grpc.NewServer()
	RegisterOrderServiceServer(srv, &OrderServiceHandler{engine: eng})
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
