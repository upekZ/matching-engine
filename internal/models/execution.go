package models

import (
	"encoding/json"
)

type ActionType string
type ExecutionReport map[string][]*Execution

const (
	ExecuteNew    ActionType = "newRequest"
	ExecuteTrade  ActionType = "trade"
	ExecuteCancel ActionType = "cancel"
	ExecuteReject ActionType = "reject"
	ExecuteAccept ActionType = "accept"
)

type Execution struct {
	ID         string      `json:"id"`
	Action     ActionType  `json:"action"`
	OStatus    OrderStatus `json:"status"`
	Symbol     string      `json:"symbol"`
	OPrice     float64     `json:"oprice"`
	TradePrice float64     `json:"tradeprice"`
	Quantity   int         `json:"quantity"`
	CumQty     int         `json:"cumQty"`
	OID        string      `json:"oid"`
	ClientOID  string      `json:"client_oid"`
	OSide      OrderSide   `json:"o_side"`
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
