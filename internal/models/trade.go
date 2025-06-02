package models

import (
	"encoding/json"
)

type Trade struct {
	ID        string  `json:"id"`
	Status    string  `json:"status"`
	Market    string  `json:"market"`
	Price     float64 `json:"price"`
	Quantity  int     `json:"quantity"`
	BuyOrder  string  `json:"buy_order"`
	SellOrder string  `json:"sell_order"`
	Timestamp int64   `json:"timestamp"`
}

func (t *Trade) ToJSON() ([]byte, error) {
	return json.Marshal(t)
}

func TradeFromJSON(data []byte) (*Trade, error) {
	var trade Trade
	err := json.Unmarshal(data, &trade)
	return &trade, err
}
