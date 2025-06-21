package engine

import (
	"container/list"
	"github.com/upekZ/matching-engine/internal/models"
)

type OrderElement struct {
	*list.Element
}

func (e *OrderElement) Value() *models.Order {
	return e.Element.Value.(*models.Order)
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

func (l *OrderList) Push(order *models.Order) *OrderElement {
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
