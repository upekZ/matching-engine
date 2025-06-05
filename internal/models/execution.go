package models

import (
	"encoding/json"
)

type ExecType string
type ExecutionReport map[string][]*Execution

const (
	ExecuteNew    ExecType = "new"
	ExecuteTrade  ExecType = "trade"
	ExecuteCancel ExecType = "canceled"
	ExecuteReject ExecType = "rejected"
	ExecuteAccept ExecType = "accept"
)

type Execution struct {
	ExecType     ExecType    `json:"exec_type"`
	OrdStatus    OrderStatus `json:"ord_status"`
	ClOrdID      string      `json:"cl_ord_id"`
	OrderID      string      `json:"orderid"`
	Symbol       string      `json:"symbol"`
	Side         OrderSide   `json:"side"`
	OrderQty     int         `json:"orderqty"`
	Price        float64     `json:"price"`
	LastQty      int         `json:"lastqty"`
	LastPx       float64     `json:"lastpx"`
	CumQty       int         `json:"cumQty"`
	LeavesQty    int         `json:"leavesQty"`
	ExecID       string      `json:"exec_id"`
	TransactTime int64       `json:"transacttime"`
	OrdType      OrderType   `json:"ord_type"`
}

func (t *Execution) ToJSON() ([]byte, error) {
	return json.Marshal(t)
}

func TradeFromJSON(data []byte) (*Execution, error) {
	var exec Execution
	err := json.Unmarshal(data, &exec)
	return &exec, err
}
