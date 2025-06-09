package models

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
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
	NewOrderState   OrderStatus = "new"
	PartiallyFilled OrderStatus = "partiallyFilled"
	Filled          OrderStatus = "filled"
	PendingCancel   OrderStatus = "pendingCancel"
	Cancelled       OrderStatus = "cancelled"
	Rejected        OrderStatus = "rejected"
)

const (
	NewLimitOrder  OrderType = "newLimitOrder"
	NewMarketOrder OrderType = "newMarketOrder"
	CancelOrder    OrderType = "cancelOrder"

	NewStopOrder     OrderType = "newStopOrder"
	NewStopLossOrder OrderType = "newStopLossOrder"
)

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
}

func AddNewLimitReq(clientID string, symbol string, side OrderSide, price float64, quantity int) *Order {
	return &Order{
		ID:           uuid.New().String(),
		ClientID:     clientID,
		Symbol:       symbol,
		Side:         side,
		Price:        price,
		Quantity:     quantity,
		FilledQty:    0,
		AvailableQty: quantity,
		Timestamp:    time.Now().Unix(),
		Status:       NewOrderState,
		ReqType:      NewLimitOrder,
	}

}

func AddNewMarketReq(clientID string, symbol string, side OrderSide, quantity int) *Order {
	return &Order{
		ID:           uuid.New().String(),
		ClientID:     clientID,
		Symbol:       symbol,
		Side:         side,
		Price:        0,
		Quantity:     quantity,
		FilledQty:    0,
		AvailableQty: quantity,
		Timestamp:    time.Now().Unix(),
		Status:       NewOrderState,
		ReqType:      NewMarketOrder,
	}

}

func AddNewStopReq(clientID string, symbol string, side OrderSide, price float64, quantity int) *Order {
	return &Order{
		ID:           uuid.New().String(),
		ClientID:     clientID,
		Symbol:       symbol,
		Side:         side,
		StopPx:       price,
		Quantity:     quantity,
		FilledQty:    0,
		AvailableQty: quantity,
		Timestamp:    time.Now().Unix(),
		Status:       NewOrderState,
		ReqType:      NewStopOrder,
	}
}

func AddNewStopLossReq(clientID string, symbol string, side OrderSide, stopPrice float64, reqPrice float64, quantity int) *Order {
	return &Order{
		ID:           uuid.New().String(),
		ClientID:     clientID,
		Symbol:       symbol,
		Side:         side,
		StopPx:       stopPrice,
		Price:        reqPrice,
		Quantity:     quantity,
		FilledQty:    0,
		AvailableQty: quantity,
		Timestamp:    time.Now().Unix(),
		Status:       NewOrderState,
		ReqType:      NewStopLossOrder,
	}
}

func OrderFromJSON(data []byte) (*Order, error) {
	var order Order
	err := json.Unmarshal(data, &order)
	return &order, err
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

func (o *Order) ExecuteNew() *Execution {

	o.Status = NewOrderState

	return &Execution{
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

func (o *Order) ExecuteCancelReq() *Execution {

	o.Status = PendingCancel

	return &Execution{
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
func (o *Order) ExecuteReject() *Execution {

	o.Status = Rejected

	return &Execution{
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

func (o *Order) ExecuteTrade(qty int, price float64) *Execution {
	o.FilledQty += qty
	o.AvailableQty -= qty

	if o.AvailableQty > 0 {
		o.Status = PartiallyFilled
	} else {
		o.Status = Filled
	}

	return &Execution{
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

func (o *Order) ExecuteCancel() *Execution {
	o.Status = Cancelled
	return &Execution{
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

func (o *Order) ExecuteAccept() *Execution {
	return &Execution{
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
