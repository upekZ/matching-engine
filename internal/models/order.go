package models

import (
	"container/list"
	"sync"
)

type Order struct {
	id        int
	market    string
	side      string
	price     float64
	quantity  int
	timestamp int64

	mu sync.Mutex
}

func NewOrder(id int, price float64, qnt int, time int64) *Order {
	return &Order{
		id:        id,
		price:     price,
		quantity:  qnt,
		timestamp: time,

		mu: sync.Mutex{},
	}
}

func (o *Order) ReduceQuantity(volume int) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.quantity >= volume {
		o.quantity -= volume
		return true
	}
	return false
}

func (o *Order) GetID() int {
	return o.id
}

func (o *Order) GetPrice() float64 {
	return o.price
}

func (o *Order) GetQty() int {
	return o.quantity
}

type OrderElement struct {
	*list.Element
}

func (e *OrderElement) Value() *Order {
	return e.Element.Value.(*Order)
}

type OrderList struct {
	*list.List
}

func NewOrderList() *OrderList {
	return &OrderList{list.New()}
}

func (e *OrderElement) Next() *OrderElement {
	if e.Element == nil {
		return nil
	}
	next := e.Element.Next()
	if next == nil {
		return nil
	}
	return &OrderElement{next}
}

func (l *OrderList) Push(order *Order) *OrderElement {
	return &OrderElement{l.List.PushBack(order)}
}

func (l *OrderList) Pop() *OrderElement {
	if l.List.Len() == 0 {
		return nil
	}
	front := l.List.Front()
	l.List.Remove(front)
	return &OrderElement{front}
}

func (l *OrderList) Front() *OrderElement {
	if l.List.Len() == 0 {
		return nil
	}
	return &OrderElement{l.List.Front()}
}

func (l *OrderList) Remove(e *OrderElement) {
	l.List.Remove(e.Element)
}
