package models

import (
	"encoding/json"
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
	NewOrderState   OrderStatus = "newOrder"
	PartiallyFilled OrderStatus = "partiallyFilled"
	Filled          OrderStatus = "filled"
	Cancelled       OrderStatus = "cancelled"
)

const (
	NewLimitOrder OrderType = "newLimitOrder"
	CancelOrder   OrderType = "cancelOrder"
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
		return SellOrder
	}
}

func (o *Order) ExecuteTrade(qty int, price float64) *Trade {
	o.FilledQty += qty
	o.AvailableQty -= qty

	if o.AvailableQty > 0 {
		o.Status = PartiallyFilled
	} else {
		o.Status = Filled
	}

	return &Trade{
		ID:        uuid.New().String(),
		Price:     price,
		Quantity:  qty,
		OrderID:   o.ID,
		ClientOID: o.ClientID,
		Timestamp: time.Now().UnixMilli(),
		Action:    TradeAction,
		Symbol:    o.Symbol,
		Status:    o.Status,
		OrderSide: o.Side,
	}
}

func (o *Order) ExecuteCancel() *Trade {
	o.Status = Cancelled
	return &Trade{
		ID:        uuid.New().String(),
		Price:     o.Price,
		Quantity:  o.Quantity,
		OrderID:   o.ID,
		ClientOID: o.ClientID,
		Timestamp: time.Now().UnixMilli(),
		Action:    CancelAction,
		Symbol:    o.Symbol,
		Status:    o.Status,
		OrderSide: o.Side,
	}
}
