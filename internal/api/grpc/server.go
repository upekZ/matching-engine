package grpc

import (
	"context"
	"fmt"
	"github.com/upekZ/matching-engine/internal/models"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"net"
)

type GlobalEngine interface {
	PlaceRequest(order *models.Order) models.Order
	PublishOrderResponse(ctx context.Context, market string, data []byte) error
	SubscribeToResponses(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error
}

type CacheStore interface {
}

type OrderServiceHandler struct {
	engine GlobalEngine
	UnimplementedOrderServiceServer
}

func (s *OrderServiceHandler) PlaceNewOrder(ctx context.Context, req *OrderRequest) (*OrderResponse, error) {

	if req.Price <= 0 || req.Quantity <= 0 || (req.Side != "buy" && req.Side != "sell") {
		return nil, status.Error(codes.InvalidArgument, "Invalid order parameters")
	}

	order := models.NewOrder(req.ClientId, req.Symbol, models.OrderSide(req.Side), float64(req.Price), int(req.Quantity), models.OrderType(req.Type))
	s.engine.PlaceRequest(order) //ToDo handle output types

	resp := &OrderResponse{
		OrderId: order.ClientID,
		Status:  OrderResponse_PENDING,
	}
	//ToDo handle errors
	resp.Status = OrderResponse_CONFIRMED
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

func (s *OrderServiceHandler) SubscribeOrderUpdates(req *MarketRequest, stream OrderService_SubscribeOrderUpdatesServer) error {
	if req.Market == "" {
		return status.Error(codes.InvalidArgument, "Symbol must be specified")
	}

	ctx := stream.Context()
	responseChannel := make(chan models.ExecutionReport, 100)
	errorChannel := make(chan error, 1)

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
		case err, ok := <-errorChannel:
			if !ok {
				return nil
			}
			return err
		case response, ok := <-responseChannel:
			if !ok {
				return nil
			}
			grpcResp := ConvertToProtoExecReport(response)
			if err := stream.Send(grpcResp); err != nil {
				return status.Errorf(codes.Internal, "Failed to send response: %v", err)
			}
		}
	}
}

func ConvertToProtoExecReport(input models.ExecutionReport) *ExecReport {
	execReport := &ExecReport{
		ExecReport: make(map[string]*TradeList, len(input)),
	}

	for key, trades := range input {
		tradeList := &TradeList{
			Trade: make([]*Trade, 0, len(input[key])),
		}
		for _, trade := range trades {
			fmt.Printf("iterating over trades: %s\n", key)
			protoTrade := &Trade{
				Id:          trade.ID,
				Status:      string(trade.OStatus),
				Symbol:      trade.Symbol,
				Price:       float32(trade.TradePrice),
				OrderPrice:  float32(trade.OPrice),
				Quantity:    int32(trade.Quantity),
				CumQuantity: int32(trade.CumQty),
				ClientId:    trade.ClientOID,
				Action:      string(trade.Action),
				Side:        string(trade.OSide),
				Timestamp:   trade.Timestamp,
				OrderId:     trade.OID,
			}
			tradeList.Trade = append(tradeList.Trade, protoTrade)
		}
		execReport.ExecReport[key] = tradeList
	}

	return execReport
}
