package models

import "encoding/json"

type OrderType string

const (
	BuyOrder  OrderType = "buy"
	SellOrder OrderType = "sell"
)

type Order struct {
	ID        string    `json:"id"`
	ClientID  string    `json:"client_id"`
	Market    string    `json:"market"`
	Side      OrderType `json:"side"`
	Price     float64   `json:"price"`
	Quantity  int       `json:"quantity"`
	Timestamp int64     `json:"timestamp"`
}

func OrderFromJSON(data []byte) (*Order, error) {
	var order Order
	err := json.Unmarshal(data, &order)
	return &order, err
}
func (o *Order) ToJSON() ([]byte, error) {
	return json.Marshal(o)
}

func (o *Order) ReduceQuantity(volume int) bool {
	if o.Quantity >= volume {
		o.Quantity -= volume
		return true
	}
	return false
}

type OrderResponse struct {
	OrderID string  `json:"order_id"`
	Status  string  `json:"status"`
	Trades  []Trade `json:"trades"`
}
