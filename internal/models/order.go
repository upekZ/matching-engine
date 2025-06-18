package models

import (
	"fmt"
	"github.com/google/uuid"
	"log"
	"time"
)

type OrderSide string
type OrderStatus string
type OrderType string
type ReqType string

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
	MarketOrder OrderType = "1"
	LimitOrder  OrderType = "2"

	// StopOrder StopLossOrder ToDo Implement stop and stop-loss
	StopOrder     OrderType = "3"
	StopLossOrder OrderType = "4"
)

const (
	NewOrder    ReqType = "NewOrder"
	CancelOrder ReqType = "CancelOrder"

	//ToDo: Amends
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
	OrdType      OrderType   `json:"ord_type"`
	ReqType      ReqType     `json:"req_type"`
}

type BaseParams struct {
	ClientID string
	Symbol   string
	ReqType  ReqType
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
		OrdType:      LimitOrder,
		ReqType:      baseParams.ReqType,
	}
}

func AddCancelReq(baseParams *BaseParams) *Order {
	return &Order{
		ID:        uuid.New().String(),
		ClientID:  baseParams.ClientID,
		Symbol:    baseParams.Symbol,
		Timestamp: time.Now().Unix(),
		Status:    NewPendingOrderState,
		ReqType:   baseParams.ReqType,
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
		OrdType:      MarketOrder,
		ReqType:      baseParams.ReqType,
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

	switch o.OrdType {
	case LimitOrder:
		if o.Price <= 0 {
			errorString += "invalid price entry\t"
		}
	case MarketOrder:

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
		OrdType:      o.OrdType,
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
		OrdType:      o.OrdType,
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
		OrdType:      o.OrdType,
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
		OrdType:      o.OrdType,
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
		OrdType:      o.OrdType,
	}
}

func (o *Order) IsReqProcessed(price float64, comp Comparator) bool {

	if o.Status == Filled {
		return true
	}

	switch o.OrdType {
	case LimitOrder:
		if !comp(o.Price, price) {
			return true
		}
	}

	return false
}
