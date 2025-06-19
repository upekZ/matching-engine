package grpc

import (
	"context"
	"github.com/upekZ/matching-engine/internal/models"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"net"
)

type GlobalEngine interface {
	SubscribeToResponses(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error
}

type OrderServiceHandler struct {
	engine GlobalEngine
	UnimplementedOrderServiceServer
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
			grpcResp := convertToProtoExecReport(response)
			if err := stream.Send(grpcResp); err != nil {
				return status.Errorf(codes.Internal, "Failed to send response: %v", err)
			}
		}
	}
}

func convertToProtoExecReport(input models.ExecutionReport) *ExecReport {
	execReport := &ExecReport{
		ExecReport: make(map[string]*ExecutionList, len(input)),
	}

	for key, execution := range input {
		execList := &ExecutionList{
			Trade: make([]*Execution, 0, len(input[key])),
		}
		for _, trade := range execution {
			protoTrade := &Execution{
				ExecutionType:      string(trade.ExecType),
				OrderStatus:        string(trade.OrdStatus),
				ClientOrderId:      trade.ClOrdID,
				OrderId:            trade.OrderID,
				Symbol:             trade.Symbol,
				Side:               string(trade.Side),
				OrderQuantity:      int32(trade.OrderQty),
				Price:              float32(trade.Price),
				LastQuantity:       int32(trade.LastQty),
				LastPx:             float32(trade.LastPx),
				CumulativeQuantity: int32(trade.CumQty),
				LeavesQuantity:     int32(trade.LeavesQty),
				ExecutionId:        trade.ExecID,
				TransactTime:       trade.TransactTime,
				OrderType:          string(trade.OrdType),
			}
			execList.Trade = append(execList.Trade, protoTrade)
		}
		execReport.ExecReport[key] = execList
	}

	return execReport
}
