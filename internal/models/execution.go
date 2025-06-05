package models

import (
	"encoding/json"
)

type ActionType string
type ExecutionReport map[string][]*Execution

const (
	ExecuteNew    ActionType = "NewOrder"
	ExecuteTrade  ActionType = "trade"
	CancelAction  ActionType = "cancel"
	ExecuteReject ActionType = "reject"
)

type Execution struct {
	ID         string      `json:"id"`
	Action     ActionType  `json:"action"`
	Status     OrderStatus `json:"status"`
	Symbol     string      `json:"symbol"`
	OrderPrice float64     `json:"orderprice"`
	TradePrice float64     `json:"tradeprice"`
	Quantity   int         `json:"quantity"`
	CumQty     int         `json:"cumQty"`
	OrderID    string      `json:"order_id"`
	ClientOID  string      `json:"client_oid"`
	OrderSide  OrderSide   `json:"order_side"`
	Timestamp  int64       `json:"timestamp"`
}

func (t *Execution) ToJSON() ([]byte, error) {
	return json.Marshal(t)
}

func TradeFromJSON(data []byte) (*Execution, error) {
	var exec Execution
	err := json.Unmarshal(data, &exec)
	return &exec, err
}
