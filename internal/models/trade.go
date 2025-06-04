package models

import (
	"encoding/json"
)

type ActionType string
type ExecutionReport map[string][]*Trade

const (
	TradeAction  ActionType = "trade"
	CancelAction ActionType = "cancel"
)

type Trade struct {
	ID         string      `json:"id"`
	Status     OrderStatus `json:"status"`
	Symbol     string      `json:"symbol"`
	OrderPrice float64     `json:"orderprice"`
	TradePrice float64     `json:"tradeprice"`
	Quantity   int         `json:"quantity"`
	CumQty     int         `json:"cumQty"`
	OrderID    string      `json:"order_id"`
	ClientOID  string      `json:"client_oid"`
	Action     ActionType  `json:"action"`
	OrderSide  OrderSide   `json:"order_side"`
	Timestamp  int64       `json:"timestamp"`
}

func (t *Trade) ToJSON() ([]byte, error) {
	return json.Marshal(t)
}

func TradeFromJSON(data []byte) (*Trade, error) {
	var trade Trade
	err := json.Unmarshal(data, &trade)
	return &trade, err
}
