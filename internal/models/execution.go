package models

import (
	"encoding/json"
)

type ExecType string
type ExecutionReport map[string][]*Execution

const (
	ExecuteNew           ExecType = "new"
	ExecuteTrade         ExecType = "trade"
	ExecuteCancel        ExecType = "canceled"
	ExecutePendingCancel ExecType = "pending_cancel"
	ExecuteReject        ExecType = "rejected"
	ExecuteAccept        ExecType = "accept"
)

type Execution struct {
	ExecType     ExecType    `json:"exec_type"`
	OrdStatus    OrderStatus `json:"ord_status"`
	ClOrdID      string      `json:"cl_ord_id"`
	OrderID      string      `json:"order_id"`
	Symbol       string      `json:"symbol"`
	Side         OrderSide   `json:"side"`
	OrderQty     int         `json:"order_qty"`
	Price        float64     `json:"price"`
	LastQty      int         `json:"last_qty"`
	LastPx       float64     `json:"last_px"`
	CumQty       int         `json:"cumQty"`
	LeavesQty    int         `json:"leavesQty"`
	ExecID       string      `json:"exec_id"`
	TransactTime int64       `json:"transact_time"`
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
