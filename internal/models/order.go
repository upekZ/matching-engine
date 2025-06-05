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
	Cancelled       OrderStatus = "cancelled"
	Rejected        OrderStatus = "rejected"
)

const (
	NewLimitOrder  OrderType = "newLimitOrder"
	NewMarketOrder OrderType = "newMarketOrder"
	CancelOrder    OrderType = "cancelOrder"
)

type Order struct {
	ID           string      `json:"id"`
	ClientID     string      `json:"client_id"`
	Symbol       string      `json:"symbol"`
	Side         OrderSide   `json:"side"`
	Price        float64     `json:"price"`
	Quantity     int         `json:"quantity"`
	FilledQty    int         `json:"filled_qty"`
	AvailableQty int         `json:"available_qty"`
	Timestamp    int64       `json:"timestamp"`
	Status       OrderStatus `json:"status"`
	ReqType      OrderType   `json:"req_type"`
}

func NewOrder(clientID string, symbol string, side OrderSide, price float64, quantity int, orderType OrderType) *Order {
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
		ReqType:      orderType,
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

	return &Execution{
		ID:         uuid.New().String(),
		OPrice:     o.Price,
		TradePrice: 0,
		Quantity:   o.Quantity,
		CumQty:     o.FilledQty,
		OID:        o.ID,
		ClientOID:  o.ClientID,
		Timestamp:  time.Now().UnixMilli(),
		Action:     ExecuteNew,
		Symbol:     o.Symbol,
		OStatus:    o.Status,
		OSide:      o.Side,
	}
}

func (o *Order) ExecuteReject() *Execution {

	o.Status = Rejected

	return &Execution{
		ID:         uuid.New().String(),
		OPrice:     o.Price,
		TradePrice: 0,
		Quantity:   0,
		CumQty:     o.FilledQty,
		OID:        o.ID,
		ClientOID:  o.ClientID,
		Timestamp:  time.Now().UnixMilli(),
		Action:     ExecuteReject,
		Symbol:     o.Symbol,
		OStatus:    o.Status,
		OSide:      o.Side,
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
		ID:         uuid.New().String(),
		OPrice:     o.Price,
		TradePrice: price,
		Quantity:   qty,
		CumQty:     o.FilledQty,
		OID:        o.ID,
		ClientOID:  o.ClientID,
		Timestamp:  time.Now().UnixMilli(),
		Action:     ExecuteTrade,
		Symbol:     o.Symbol,
		OStatus:    o.Status,
		OSide:      o.Side,
	}
}

func (o *Order) ExecuteCancel() *Execution {
	o.Status = Cancelled
	return &Execution{
		ID:        uuid.New().String(),
		OPrice:    o.Price,
		Quantity:  o.Quantity,
		CumQty:    o.FilledQty,
		OID:       o.ID,
		ClientOID: o.ClientID,
		Timestamp: time.Now().UnixMilli(),
		Action:    ExecuteCancel,
		Symbol:    o.Symbol,
		OStatus:   o.Status,
		OSide:     o.Side,
	}
}

func (o *Order) ExecuteAccept() *Execution {
	return &Execution{
		ID:        uuid.New().String(),
		OPrice:    o.Price,
		Quantity:  o.Quantity,
		CumQty:    o.FilledQty,
		OID:       o.ID,
		ClientOID: o.ClientID,
		Timestamp: time.Now().UnixMilli(),
		Action:    ExecuteAccept,
		Symbol:    o.Symbol,
		OStatus:   o.Status,
		OSide:     o.Side,
	}
}
