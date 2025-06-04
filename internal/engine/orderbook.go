package engine

import (
	"encoding/json"
	"fmt"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
)

func (ob *OrderBook) addRequest(order *models.Order, returnCmp models.Comparator) ([]*models.Trade, error) {
	trades, err := ob.matchOrder(order, returnCmp)

	if err != nil {
		return nil, err
	}

	if order.ReqType == models.NewLimitOrder && order.Status != models.Filled {

		if err := ob.addToOrderBook(order); err != nil {
			log.Printf("Error adding order to orderbook: %v", err)
			return trades, fmt.Errorf("order request partially executed")
		}
	}
	return trades, nil
}

func (ob *OrderBook) AddBuyRequest(order *models.Order) ([]*models.Trade, error) {
	return ob.addRequest(order, models.Lesser)
}

func (ob *OrderBook) AddSellRequest(order *models.Order) ([]*models.Trade, error) {

	return ob.addRequest(order, models.Greater)
}

func (ob *OrderBook) addToOrderBook(order *models.Order) error {

	priceMap, priceList := ob.getContainers(order.Side)

	if priceMap[order.Price] == nil {
		priceMap[order.Price] = models.NewOrderList()
		priceList.Put(order.Price, true)
	}
	priceRef := priceMap[order.Price]

	element := priceRef.Push(order)

	ob.OrderIndex[order.ID] = element
	ob.ClientIDs[order.ClientID] = order.ID
	return nil
}

func (ob *OrderBook) CancelOrder(order *models.Order) ([]*models.Trade, error) {

	orderID, ok := ob.ClientIDs[order.ClientID]
	if !ok {
		return nil, fmt.Errorf("order %s not found in order-book", order.ClientID)
	}

	orderInfo, exists := ob.OrderIndex[orderID]
	if !exists {
		return nil, fmt.Errorf("order: %s doesn't exist", order.ID)
	}

	orderList, priceList := ob.getContainers(order.Side)

	price := orderInfo.Value().Price
	list := orderList[price]
	list.Remove(orderInfo)

	if list.Len() == 0 {
		delete(orderList, price)
		priceList.Remove(price)
	}

	delete(ob.OrderIndex, order.ID)
	delete(ob.ClientIDs, order.ClientID)

	trades := make([]*models.Trade, 1)
	trades = append(trades, order.ExecuteCancel())

	return trades, nil
}

func (ob *OrderBook) matchOrder(order *models.Order, returnCmp models.Comparator) ([]*models.Trade, error) {

	orderPrice := order.Price

	_, priceList := ob.getContainers(order.GetOppositeOrderType())

	keys := priceList.Keys()

	allTrades := make([]*models.Trade, 0)

	for _, key := range keys {

		currentPrice := key.(float64)

		if !returnCmp(currentPrice, orderPrice) || order.Status == models.Filled {
			break
		}

		if trades, err := ob.matchOrdersInPrice(currentPrice, order); err != nil {
			return allTrades, err
		} else {
			allTrades = append(allTrades, trades...)
		}
	}
	return allTrades, nil
}

func (ob *OrderBook) matchOrdersInPrice(price float64, order *models.Order) ([]*models.Trade, error) {

	ordersByPrice, _ := ob.getContainers(order.Side)

	priceInfo := ordersByPrice[price]

	e := priceInfo.Front()

	matchedTrades := make([]*models.Trade, 0)

	for e != nil && order.Status != models.Filled {

		bookOrder := e.Value()

		tradeQty := 0

		if bookOrder.Quantity <= order.AvailableQty {
			tradeQty = bookOrder.Quantity
		} else {
			tradeQty = order.AvailableQty
		}

		matchedTrades = append(matchedTrades, bookOrder.ExecuteTrade(tradeQty, price))
		matchedTrades = append(matchedTrades, order.ExecuteTrade(tradeQty, price))
	}

	return matchedTrades, nil
}

func (ob *OrderBook) removeOrder(order *models.Order) error {

	orderID, ok := ob.ClientIDs[order.ClientID]
	if !ok {
		return fmt.Errorf("order %s not found in order-book", order.ClientID)
	}

	orderInfo, exists := ob.OrderIndex[orderID]
	if !exists {
		return fmt.Errorf("order: %s doesn't exist", order.ID)
	}

	orderList, priceList := ob.getContainers(order.Side)

	price := orderInfo.Value().Price
	bucket := orderList[price]
	bucket.Remove(orderInfo)

	if bucket.Len() == 0 {
		delete(orderList, price)
		priceList.Remove(price)
	}

	delete(ob.OrderIndex, order.ID)
	delete(ob.ClientIDs, order.ClientID)

	return nil
}

func (ob *OrderBook) ToJSON() ([]byte, error) {
	return json.Marshal(ob)
}
