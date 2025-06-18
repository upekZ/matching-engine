package models

import (
	"encoding/json"
)

type ExecType string
type ExecutionReport map[string][]*Execution

const (
	ExecuteNew           ExecType = "0"
	ExecuteTrade         ExecType = "F"
	ExecuteCancel        ExecType = "4"
	ExecutePendingCancel ExecType = "6"
	ExecuteReject        ExecType = "8"
)

type Execution struct {
	ExecType     ExecType    `json:"<150>"`
	OrdStatus    OrderStatus `json:"<39>"`
	ClOrdID      string      `json:"<11>"`
	OrderID      string      `json:"<37>"`
	Symbol       string      `json:"<55>"`
	Side         OrderSide   `json:"<54>"`
	OrderQty     int         `json:"<38>"`
	Price        float64     `json:"<44>"`
	LastQty      int         `json:"<29>"`
	LastPx       float64     `json:"<31>"`
	CumQty       int         `json:"<14>"`
	LeavesQty    int         `json:"<151>"`
	ExecID       string      `json:"<17>"`
	TransactTime int64       `json:"<60>"`
	OrdType      OrderType   `json:"<40>"`
}

func (t *Execution) ToJSON() ([]byte, error) {
	return json.Marshal(t)
}

func TradeFromJSON(data []byte) (*Execution, error) {
	var exec Execution
	err := json.Unmarshal(data, &exec)
	return &exec, err
}
