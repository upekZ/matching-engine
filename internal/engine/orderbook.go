package engine

import (
	"encoding/json"
	"fmt"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
)

func (ob *OrderBook) processRequest(order *models.Order, returnCmp models.Comparator) ([]*models.Execution, error) {

	executions := make([]*models.Execution, 3)
	executions = append(executions, order.ExecuteNew())

	if _, err := order.IsValidReq(); err != nil {
		return executions, err
	}

	executions = append(executions, ob.matchOrder(order, returnCmp)...)

	if order.ReqType == models.NewLimitOrder && order.Status != models.Filled {
		executions = append(executions, ob.matchOrder(order, returnCmp)...)
	}
	return executions, nil
}

func (ob *OrderBook) AddBuyRequest(order *models.Order) ([]*models.Execution, error) {
	return ob.processRequest(order, models.Lesser)
}

func (ob *OrderBook) AddSellRequest(order *models.Order) ([]*models.Execution, error) {

	return ob.processRequest(order, models.Greater)
}

func (ob *OrderBook) addToOrderBook(order *models.Order) *models.Execution {

	priceMap, priceList := ob.getContainers(order.Side)

	if priceMap[order.Price] == nil {
		priceMap[order.Price] = models.NewOrderList()
		priceList.Put(order.Price, true)
	}

	priceRef := priceMap[order.Price]

	element := priceRef.Push(order)

	ob.OrderIndex[order.ID] = element
	ob.ClientIDs[order.ClientID] = order.ID

	return order.ExecuteAccepted()
}

func (ob *OrderBook) CancelOrder(order *models.Order) ([]*models.Execution, error) {

	execution := make([]*models.Execution, 1)
	execution = append(execution, order.ExecuteNew())

	if err := ob.removeOrder(*order); err != nil {
		execution = append(execution, order.ExecuteReject())
		return execution, fmt.Errorf("cancel failure: %s", err.Error())
	}

	execution = append(execution, order.ExecuteCancel())

	return execution, nil
}

func (ob *OrderBook) matchOrder(order *models.Order, returnCmp models.Comparator) []*models.Execution {

	orderPrice := order.Price

	_, priceList := ob.getContainers(order.GetOppositeOrderType())

	keys := priceList.Keys()

	allTrades := make([]*models.Execution, 0)

	filledOrders := make([]models.Order, 0)

	defer func() {
		//filled orders are removed from Order-book at the end after request completion
		for _, order := range filledOrders {
			if err := ob.removeOrder(order); err != nil {
				log.Printf("Error removing order[%s] from orderbook: %v", order.ClientID, err)
			}
		}
	}()

	for _, key := range keys {

		currentPrice := key.(float64)

		if !returnCmp(currentPrice, orderPrice) || order.Status == models.Filled {
			break
		}

		trades, removeOrders := ob.matchOrdersInPrice(currentPrice, order)
		filledOrders = append(filledOrders, removeOrders...)

		allTrades = append(allTrades, trades...)
	}

	return allTrades
}

func (ob *OrderBook) matchOrdersInPrice(price float64, order *models.Order) ([]*models.Execution, []models.Order) {

	filledOrders := make([]models.Order, 1)

	ordersByPrice, _ := ob.getContainers(order.GetOppositeOrderType())

	priceInfo := ordersByPrice[price]

	e := priceInfo.Front()

	matchedTrades := make([]*models.Execution, 0)

	for e != nil && order.Status != models.Filled {

		bookOrder := e.Value()

		tradeQty := 0

		if bookOrder.Quantity <= order.AvailableQty {
			filledOrders = append(filledOrders, *bookOrder) //only add orders in order-book as filled orders
			tradeQty = bookOrder.Quantity
		} else {
			tradeQty = order.AvailableQty
		}

		matchedTrades = append(matchedTrades, bookOrder.ExecuteTrade(tradeQty, price))
		matchedTrades = append(matchedTrades, order.ExecuteTrade(tradeQty, price))

		e = e.Next()
	}

	return matchedTrades, filledOrders
}

func (ob *OrderBook) ProcessExecutionsToReport(trades []*models.Execution) models.ExecutionReport {

	execReports := make(models.ExecutionReport, 2)

	for _, t := range trades {
		execReports[t.ClientOID] = append(execReports[t.ClientOID], t)
	}

	return execReports
}

func (ob *OrderBook) removeOrder(order models.Order) error {

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
