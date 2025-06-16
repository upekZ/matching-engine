package models

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"log"
	"time"
)

type OrderSide string
type OrderStatus string
type OrderType string

const (
	BuyOrder  OrderSide = "1"
	SellOrder OrderSide = "2"
)

const (
	NewPendingOrderState OrderStatus = "A"
	NewOrderState        OrderStatus = "0"
	PartiallyFilled      OrderStatus = "1"
	Filled               OrderStatus = "2"
	Cancelled            OrderStatus = "4"
	PendingCancel        OrderStatus = "6"
	Rejected             OrderStatus = "8"
)

const (
	NewLimitOrder  OrderType = "2"
	NewMarketOrder OrderType = "1"
	CancelOrder    OrderType = "cancelOrder"

	// NewStopOrder NewStopLossOrder ToDo Implement stop and stop-loss
	NewStopOrder     OrderType = "3"
	NewStopLossOrder OrderType = "4"
)

type CacheStore interface {
	SaveExecutions(execs *Execution) error
}

type MessageBroker interface {
	PublishOrderResponse(ctx context.Context, market string, execs ExecutionReport) error
}

type Order struct {
	ID           string      `json:"id"`
	ClientID     string      `json:"client_id"`
	Symbol       string      `json:"symbol"`
	Side         OrderSide   `json:"side"`
	Price        float64     `json:"price"`
	StopPx       float64     `json:"stop_px"`
	Quantity     int         `json:"quantity"`
	FilledQty    int         `json:"filled_qty"`
	AvailableQty int         `json:"available_qty"`
	Timestamp    int64       `json:"timestamp"`
	Status       OrderStatus `json:"status"`
	ReqType      OrderType   `json:"req_type"`

	executions []*Execution
	msgBroker  MessageBroker
	store      CacheStore
}

type BaseParams struct {
	ClientID string
	Symbol   string

	MsgBroker MessageBroker
	Store     CacheStore
}

func AddNewLimitReq(baseParams *BaseParams, side OrderSide, price float64, quantity int) *Order {
	return &Order{
		ID:           uuid.New().String(),
		ClientID:     baseParams.ClientID,
		Symbol:       baseParams.Symbol,
		Side:         side,
		Price:        price,
		Quantity:     quantity,
		AvailableQty: quantity,
		Timestamp:    time.Now().Unix(),
		Status:       NewPendingOrderState,
		ReqType:      NewLimitOrder,

		msgBroker: baseParams.MsgBroker,
		store:     baseParams.Store,
	}
}

func AddCancelReq(baseParams *BaseParams) *Order {
	return &Order{
		ID:        uuid.New().String(),
		ClientID:  baseParams.ClientID,
		Symbol:    baseParams.Symbol,
		Timestamp: time.Now().Unix(),
		Status:    NewPendingOrderState,
		ReqType:   CancelOrder,

		msgBroker: baseParams.MsgBroker,
		store:     baseParams.Store,
	}

}

func AddNewMarketReq(baseParams *BaseParams, side OrderSide, quantity int) *Order {
	return &Order{
		ID:           uuid.New().String(),
		ClientID:     baseParams.ClientID,
		Symbol:       baseParams.Symbol,
		Side:         side,
		Quantity:     quantity,
		AvailableQty: quantity,
		Timestamp:    time.Now().Unix(),
		Status:       NewPendingOrderState,
		ReqType:      NewMarketOrder,

		msgBroker: baseParams.MsgBroker,
		store:     baseParams.Store,
	}
}

func (o *Order) GetOppositeOrderType() OrderSide {
	if o.Side == BuyOrder {
		return SellOrder
	} else {
		return BuyOrder
	}
}

func (o *Order) ValidateReq() error {

	var errorString string

	if o.Quantity <= 0 {
		errorString += "invalid quantity entry\t"
	}

	switch o.ReqType {
	case NewLimitOrder:
		if o.Price <= 0 {
			errorString += "invalid price entry\t"
		}
	case NewMarketOrder:
	case CancelOrder:

	default:
		errorString += "invalid req_type\t"
	}

	switch o.Side {
	case BuyOrder:
	case SellOrder:

	default:
		errorString += "invalid side\t"

	}

	if errorString != "" {
		log.Printf("invalid order params: %s", errorString)
		return fmt.Errorf("invalid order params")
	}
	return nil

}

func (o *Order) ExecuteNew() {
	o.Status = NewOrderState
	o.executions = append(o.executions, &Execution{
		ExecType:     ExecuteNew,
		OrdStatus:    o.Status,
		ClOrdID:      o.ClientID,
		OrderID:      o.ID,
		Symbol:       o.Symbol,
		Side:         o.Side,
		OrderQty:     o.Quantity,
		Price:        o.Price,
		LastQty:      0,
		LastPx:       0,
		CumQty:       o.FilledQty,
		LeavesQty:    o.AvailableQty,
		ExecID:       uuid.New().String(),
		TransactTime: time.Now().Unix(),
		OrdType:      o.ReqType,
	})
}

func (o *Order) ExecuteCancelReq() {
	o.Status = PendingCancel
	o.executions = append(o.executions, &Execution{
		ExecType:     ExecutePendingCancel,
		OrdStatus:    o.Status,
		ClOrdID:      o.ClientID,
		OrderID:      o.ID,
		Symbol:       o.Symbol,
		Side:         o.Side,
		OrderQty:     o.Quantity,
		Price:        o.Price,
		LastQty:      0,
		LastPx:       0,
		CumQty:       o.FilledQty,
		LeavesQty:    o.AvailableQty,
		ExecID:       uuid.New().String(),
		TransactTime: time.Now().Unix(),
		OrdType:      o.ReqType,
	})
}
func (o *Order) ExecuteReject() {
	o.Status = Rejected
	o.executions = append(o.executions, &Execution{
		ExecType:     ExecuteReject,
		OrdStatus:    o.Status,
		ClOrdID:      o.ClientID,
		OrderID:      o.ID,
		Symbol:       o.Symbol,
		Side:         o.Side,
		OrderQty:     o.Quantity,
		Price:        o.Price,
		LastQty:      0,
		LastPx:       0,
		CumQty:       o.FilledQty,
		LeavesQty:    o.AvailableQty,
		ExecID:       uuid.New().String(),
		TransactTime: time.Now().Unix(),
		OrdType:      o.ReqType,
	})
}

func (o *Order) ExecuteTrade(qty int, price float64) {
	o.FilledQty += qty
	o.AvailableQty -= qty

	if o.AvailableQty > 0 {
		o.Status = PartiallyFilled
	} else {
		o.Status = Filled
	}

	o.executions = append(o.executions, &Execution{
		ExecType:     ExecuteTrade,
		OrdStatus:    o.Status,
		ClOrdID:      o.ClientID,
		OrderID:      o.ID,
		Symbol:       o.Symbol,
		Side:         o.Side,
		OrderQty:     o.Quantity,
		Price:        o.Price,
		LastQty:      qty,
		LastPx:       price,
		CumQty:       o.FilledQty,
		LeavesQty:    o.AvailableQty,
		ExecID:       uuid.New().String(),
		TransactTime: time.Now().Unix(),
		OrdType:      o.ReqType,
	})
}

func (o *Order) ExecuteCancel() {
	o.Status = Cancelled
	o.executions = append(o.executions, &Execution{
		ExecType:     ExecuteCancel,
		OrdStatus:    o.Status,
		ClOrdID:      o.ClientID,
		OrderID:      o.ID,
		Symbol:       o.Symbol,
		Side:         o.Side,
		OrderQty:     o.Quantity,
		Price:        o.Price,
		LastQty:      0,
		LastPx:       0,
		CumQty:       o.FilledQty,
		LeavesQty:    o.AvailableQty,
		ExecID:       uuid.New().String(),
		TransactTime: time.Now().Unix(),
		OrdType:      o.ReqType,
	})
}

func (o *Order) ExecuteAccept() {
	o.executions = append(o.executions, &Execution{
		ExecType:     ExecuteAccept,
		OrdStatus:    o.Status,
		ClOrdID:      o.ClientID,
		OrderID:      o.ID,
		Symbol:       o.Symbol,
		Side:         o.Side,
		OrderQty:     o.Quantity,
		Price:        o.Price,
		LastQty:      0,
		LastPx:       0,
		CumQty:       o.FilledQty,
		LeavesQty:    o.AvailableQty,
		ExecID:       uuid.New().String(),
		TransactTime: time.Now().Unix(),
		OrdType:      o.ReqType,
	})
}

func (o *Order) ProcessExecutions() {
	for _, exec := range o.executions {
		if err := o.store.SaveExecutions(exec); err != nil {
			log.Printf("failed to save trades: %v", err)
		}
	}
	execReport := make(map[string][]*Execution, 1)
	execReport[o.ClientID] = o.executions

	if err := o.msgBroker.PublishOrderResponse(context.Background(), o.Symbol, execReport); err != nil {
		log.Printf("failed to publish order response: %v", err)
	}

	o.executions = nil
}

func (o *Order) IsReqProcessed(price float64, comp Comparator) bool {

	if o.Status == Filled {
		return true
	}

	switch o.ReqType {
	case NewLimitOrder:
		if !comp(o.Price, price) {
			return true
		}
	}

	return false
}
