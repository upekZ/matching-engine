package models

import "encoding/json"

type Trade struct {
	TakerOrderID string `json:"taker_order_id"`
	MakerOrderID string `json:"maker_order_id"`
	Amount       uint64 `json:"amount"`
	Price        uint64 `json:"price"`
}

func (t *Trade) FromJSON(msg []byte) error {
	return json.Unmarshal(msg, t)
}

func (t *Trade) ToJSON() []byte {
	str, _ := json.Marshal(t)
	return str
}
