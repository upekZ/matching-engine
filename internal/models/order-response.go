package models

type OrderResponse struct {
	orderID string
	status  string
	trades  []*Trade
}

func NewOrderResponse(orderID string) *OrderResponse {
	return &OrderResponse{
		orderID: orderID,
		status:  "new",
		trades:  make([]*Trade, 0),
	}
}

func (o *OrderResponse) OrderID() string {
	return o.orderID
}

func (o *OrderResponse) Status() string {
	return o.status
}

func (o *OrderResponse) SetStatus(status string) {
	o.status = status
}

func (o *OrderResponse) SetTrades(trades []*Trade) {
	o.trades = trades
}
