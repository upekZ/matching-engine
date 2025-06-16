package engine

import (
	"context"
	"errors"
	"fmt"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
)

func (ob *orderBook) runOrderBook(ctx context.Context, orderChan chan orderRequest) {
	for {
		select {
		case req := <-orderChan:

			if req.ReqType != models.CancelOrder {
				switch req.Side {
				case models.SellOrder:
					ob.addSellRequest(req)
				case models.BuyOrder:
					ob.addBuyRequest(req)
				default:
					log.Printf("Unknown order[%s] side %s", req.ClientID, req.Side)
					continue
				}
			} else {
				ob.cancelOrder(req)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (ob *orderBook) processRequest(order *models.Order, returnCmp models.Comparator) {

	order.ExecuteNew()

	defer order.ProcessExecutions()

	if err := order.ValidateReq(); err != nil {
		order.ExecuteReject()
		return
	}

	if err := ob.validateReqInOB(order); err != nil {
		order.ExecuteReject()
		return
	}

	ob.matchOrder(order, returnCmp)

	if order.ReqType == models.NewLimitOrder && (order.Status == models.PartiallyFilled || order.Status == models.NewOrderState) {
		ob.addToOrderBook(order)
	}

	return
}

func (ob *orderBook) addBuyRequest(order *models.Order) {
	ob.processRequest(order, models.Greater)
}

func (ob *orderBook) addSellRequest(order *models.Order) {
	ob.processRequest(order, models.Lesser)
}

func (ob *orderBook) cancelOrder(order *models.Order) {

	defer order.ProcessExecutions()

	order.ExecuteCancelReq()

	if err := ob.removeOrder(order); err != nil {
		order.ExecuteReject()
		log.Printf("Could not cancel order: %v", err)
		return
	}
	order.ExecuteCancel()
}

func (ob *orderBook) addToOrderBook(order *models.Order) {

	priceMap, priceList := ob.getOBContainers(order.Side)

	if priceMap[order.Price] == nil {
		priceMap[order.Price] = models.NewOrderList()
		priceList.Put(order.Price, true)
	}

	priceRef := priceMap[order.Price]

	element := priceRef.Push(order)

	ob.orderIndex[order.ID] = element
	ob.clientIDs[order.ClientID] = order.ID

	order.ExecuteAccept()
}

func (ob *orderBook) matchOrder(order *models.Order, returnCmp models.Comparator) {

	_, priceList := ob.getOBContainers(order.GetOppositeOrderType())

	filledOrders := make([]*models.Order, 0, 1)

	defer func() {
		//filled orders are removed from Order-book at the end after matching completion
		for _, order := range filledOrders {
			if err := ob.removeOrder(order); err != nil {
				log.Printf("Error removing order[%s] from orderbook: %v", order.ClientID, err)
			}
		}
	}()

	keys := priceList.Keys()
	for _, key := range keys {
		currentPrice := key.(float64)

		if order.IsReqProcessed(currentPrice, returnCmp) {
			break
		}

		toRemoveOrders := ob.matchOrdersInPrice(currentPrice, order)
		filledOrders = append(filledOrders, toRemoveOrders...)
	}
}

func (ob *orderBook) matchOrdersInPrice(price float64, order *models.Order) []*models.Order {

	filledOrders := make([]*models.Order, 0, 1)

	ordersByPrice, _ := ob.getOBContainers(order.GetOppositeOrderType())

	priceInfo := ordersByPrice[price]

	e := priceInfo.Front()

	for e != nil && order.Status != models.Filled {

		bookOrder := e.Value()

		tradeQty := 0

		if bookOrder.Quantity <= order.AvailableQty {
			filledOrders = append(filledOrders, bookOrder) //only add orders in order-book as filled orders
			tradeQty = bookOrder.Quantity
		} else {
			tradeQty = order.AvailableQty
		}
		bookOrder.ExecuteTrade(tradeQty, price)
		bookOrder.ProcessExecutions()

		order.ExecuteTrade(tradeQty, price)

		e = e.Next()
	}

	return filledOrders
}

func (ob *orderBook) validateReqInOB(order *models.Order) error {

	if order.ReqType != models.CancelOrder {
		if _, exists := ob.clientIDs[order.ClientID]; exists {
			return errors.New("duplicate order id")
		}
	}

	return nil
}

func (ob *orderBook) removeOrder(order *models.Order) error {

	orderID, ok := ob.clientIDs[order.ClientID]
	if !ok {
		return fmt.Errorf("order %s not found in order-book", order.ClientID)
	}

	orderInfo, exists := ob.orderIndex[orderID]
	if !exists {
		return fmt.Errorf("order: %s doesn't exist", order.ID)
	}

	orderList, priceList := ob.getOBContainers(orderInfo.Value().Side)

	price := orderInfo.Value().Price
	bucket := orderList[price]
	bucket.Remove(orderInfo)

	if bucket.Len() == 0 {
		delete(orderList, price)
		priceList.Remove(price)
	}

	delete(ob.orderIndex, order.ID)
	delete(ob.clientIDs, order.ClientID)

	return nil
}
