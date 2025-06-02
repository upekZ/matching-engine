package grpc

import (
	"context"
	"encoding/json"
	"fmt"
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
	CancelOrder(order *models.Order) (*models.TradeManager, error)
	PublishOrderResponse(ctx context.Context, market string, data []byte) error
	SubscribeToResponses(ctx context.Context, market string, responseChannel chan models.OrderResponse) error
}

type CacheStore interface {
}

type OrderServiceHandler struct {
	engine GlobalEngine
	UnimplementedOrderServiceServer
}

func (s *OrderServiceHandler) PlaceOrder(ctx context.Context, req *OrderRequest) (*OrderResponse, error) {

	if req.Price <= 0 || req.Quantity <= 0 || (req.Side != "buy" && req.Side != "sell") {
		return nil, status.Error(codes.InvalidArgument, "Invalid order parameters")
	}

	order := &models.Order{
		ID:        uuid.New().String(),
		ClientID:  req.ClientId,
		Market:    req.GetMarket(),
		Side:      models.OrderType(req.GetSide()),
		Price:     req.GetPrice(),
		Quantity:  int(req.Quantity),
		Timestamp: time.Now().UnixNano(),
	}

	trades, err := s.engine.PlaceOrder(order)

	if err != nil {
		return nil, err
	}

	response := s.convertToResponse(order, trades)

	data, err := json.Marshal(response)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to serialize response")
	}
	if err := s.engine.PublishOrderResponse(ctx, req.Market, data); err != nil {
		log.Printf("Failed to publish data: %v", err)
		return nil, status.Error(codes.Internal, "Failed to forward response")
	}

	return response, nil
}

func (s *OrderServiceHandler) convertToResponse(order *models.Order, trades *models.TradeManager) *OrderResponse {

	resp := &OrderResponse{
		OrderId: order.ClientID,
		Status:  "new",
	}
	if trades.GetVolume() > 0 {
		if order.Quantity > trades.GetVolume() {
			resp.Status = "partially_filled"
		} else {
			resp.Status = "filled"
		}
		resp.Trades = make([]*Trade, len(trades.GetTrades()))
		for i, t := range trades.GetTrades() {
			resp.Trades[i] = &Trade{
				Id:        t.ID,
				Market:    order.Market,
				Price:     t.Price,
				Quantity:  int32(t.Quantity),
				BuyOrder:  t.BuyOrder,
				SellOrder: t.SellOrder,
				Timestamp: t.Timestamp,
			}
		}
	}

	return resp
}

func (s *OrderServiceHandler) CancelOrder(ctx context.Context, req *OrderRequest) (*OrderResponse, error) {

	order := &models.Order{
		ID:        uuid.New().String(),
		ClientID:  req.ClientId,
		Market:    req.GetMarket(),
		Side:      models.OrderType(req.GetSide()),
		Price:     req.GetPrice(),
		Quantity:  int(req.Quantity),
		Timestamp: time.Now().UnixNano(),
	}

	trades, err := s.engine.CancelOrder(order)

	if err != nil {
		log.Printf(status.Errorf(codes.Internal, "Failed to cancel order %v", err).Error())
		return nil, status.Error(codes.Internal, "Failed to cancel order")
	}
	resp := &OrderResponse{
		OrderId: order.ID,
		Status:  "new",
	}
	if trades.GetVolume() > 0 {
		resp.Status = "cancelled"
		resp.Trades = make([]*Trade, len(trades.GetTrades()))
		for i, t := range trades.GetTrades() {
			resp.Trades[i] = &Trade{
				Id:        t.ID,
				Market:    order.Market,
				Price:     t.Price,
				Quantity:  int32(t.Quantity),
				BuyOrder:  t.BuyOrder,
				SellOrder: t.SellOrder,
				Timestamp: t.Timestamp,
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

func (s *OrderServiceHandler) ListenToOrders(ctx context.Context, market string, responseChannel chan models.OrderResponse) error {
}

func (s *OrderServiceHandler) SubscribeOrderUpdates(req *MarketRequest, stream OrderService_SubscribeOrderUpdatesServer) error {
	if req.Market == "" {
		return status.Error(codes.InvalidArgument, "Market must be specified")
	}

	ctx := stream.Context()
	responseChannel := make(chan models.OrderResponse, 100)
	errorChannel := make(chan error, 1)

	fmt.Printf("runnig grpcs")

	go func() {
		err := s.engine.SubscribeToResponses(ctx, req.Market, responseChannel)
		if err != nil {
			errorChannel <- err
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errorChannel:
			return err
		case response := <-responseChannel:
			grpcResp := &OrderResponse{
				OrderId: response.OrderID,
				Status:  response.Status,
				Trades:  make([]*Trade, len(response.Trades)),
			}
			for i, t := range response.Trades {
				grpcResp.Trades[i] = &Trade{
					Id:        t.ID,
					Market:    t.Market,
					Price:     t.Price,
					Quantity:  int32(t.Quantity),
					BuyOrder:  t.BuyOrder,
					SellOrder: t.SellOrder,
					Timestamp: t.Timestamp,
				}
			}
			if err := stream.Send(grpcResp); err != nil {
				return status.Errorf(codes.Internal, "Failed to send response: %v", err)
			}
		}
	}
}
