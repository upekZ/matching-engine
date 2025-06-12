package models

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"log"
	"time"
)

type OrderSide string
type OrderStatus string
type OrderType string

const (
	BuyOrder  OrderSide = "buy"
	SellOrder OrderSide = "sell"
)

const (
	NewPendingOrderState OrderStatus = "newPending"
	NewOrderState        OrderStatus = "new"
	PartiallyFilled      OrderStatus = "partiallyFilled"
	Filled               OrderStatus = "filled"
	PendingCancel        OrderStatus = "pendingCancel"
	Cancelled            OrderStatus = "cancelled"
	Rejected             OrderStatus = "rejected"
)

const (
	NewLimitOrder  OrderType = "newLimitOrder"
	NewMarketOrder OrderType = "newMarketOrder"
	CancelOrder    OrderType = "cancelOrder"

	NewStopOrder     OrderType = "newStopOrder"
	NewStopLossOrder OrderType = "newStopLossOrder"
)

type CacheStore interface {
	SaveTrades(trades *Execution) error
}

type MessageBroker interface {
	PublishOrderResponse(ctx context.Context, market string, execs []*Execution) error
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

	executions chan *Execution
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
		ReqType:   NewLimitOrder,

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

func AddNewStopReq(baseParams *BaseParams, side OrderSide, price float64, quantity int) *Order {
	return &Order{
		ID:           uuid.New().String(),
		ClientID:     baseParams.ClientID,
		Symbol:       baseParams.Symbol,
		Side:         side,
		StopPx:       price,
		Quantity:     quantity,
		AvailableQty: quantity,
		Timestamp:    time.Now().Unix(),
		Status:       NewPendingOrderState,
		ReqType:      NewStopOrder,

		msgBroker: baseParams.MsgBroker,
		store:     baseParams.Store,
	}
}

func AddNewStopLossReq(baseParams *BaseParams, side OrderSide, stopPrice float64, reqPrice float64, quantity int) *Order {
	return &Order{
		ID:           uuid.New().String(),
		ClientID:     baseParams.ClientID,
		Symbol:       baseParams.Symbol,
		Side:         side,
		StopPx:       stopPrice,
		Price:        reqPrice,
		Quantity:     quantity,
		AvailableQty: quantity,
		Timestamp:    time.Now().Unix(),
		Status:       NewPendingOrderState,
		ReqType:      NewStopLossOrder,

		msgBroker: baseParams.MsgBroker,
		store:     baseParams.Store,
	}
}

func (o *Order) ToJSON() ([]byte, error) {
	return json.Marshal(o)
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

	if o.Price <= 0 {
		errorString += "invalid price entry\t"
	}

	if o.Quantity <= 0 {
		errorString += "invalid quantity entry\t"
	}

	switch o.ReqType {
	case NewLimitOrder:
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
		return fmt.Errorf(errorString)
	}
	return nil

}

func (o *Order) ExecuteNew() {
	o.executions <- &Execution{
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
	}
}

func (o *Order) ExecuteCancelReq() {
	o.Status = PendingCancel
	o.executions <- &Execution{
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
	}
}
func (o *Order) ExecuteReject() {
	o.Status = Rejected
	o.executions <- &Execution{
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
	}
}

func (o *Order) ExecuteTrade(qty int, price float64) {
	o.FilledQty += qty
	o.AvailableQty -= qty

	if o.AvailableQty > 0 {
		o.Status = PartiallyFilled
	} else {
		o.Status = Filled
	}

	o.executions <- &Execution{
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
	}
}

func (o *Order) ExecuteCancel() {
	o.Status = Cancelled
	o.executions <- &Execution{
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
	}
}

func (o *Order) ExecuteAccept() {
	o.executions <- &Execution{
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
	}
}

func (o *Order) ProcessExecutions() {
	for exec := range o.executions {
		if err := o.store.SaveTrades(exec); err != nil {
			log.Printf("failed to save trades: %v", err)
		}
	}
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
