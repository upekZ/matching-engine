package engine

import (
	"context"
	"errors"
	"fmt"
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
)

type PriceToOrderMap map[float64]*OrderList
type sellOrders struct {
	ordersByPrice PriceToOrderMap
	orderedPrices *rbt.Tree
}

func newSellContainers() *sellOrders {

	return &sellOrders{
		ordersByPrice: make(PriceToOrderMap),
		orderedPrices: rbt.NewWith(models.SellComparator),
	}
}

func (s *sellOrders) getContainers() (PriceToOrderMap, *rbt.Tree) {
	return s.ordersByPrice, s.orderedPrices
}

type buyOrders struct {
	ordersByPrice PriceToOrderMap
	orderedPrices *rbt.Tree
}

func (b *buyOrders) getContainers() (PriceToOrderMap, *rbt.Tree) {
	return b.ordersByPrice, b.orderedPrices
}

func newBuyContainers() *buyOrders {

	return &buyOrders{
		ordersByPrice: make(PriceToOrderMap),
		orderedPrices: rbt.NewWith(models.BuyComparator),
	}
}

type orderBook struct {
	market              string
	sellOrderContainers *sellOrders
	buyOrderContainers  *buyOrders
	clientIDs           map[string]*OrderElement
	handler             models.ExecHandler
}

func newOrderBook(ctx context.Context, market string, handler models.ExecHandler) chan *models.Order {

	ob := &orderBook{
		market: market,

		sellOrderContainers: newSellContainers(),
		buyOrderContainers:  newBuyContainers(),

		clientIDs: make(map[string]*OrderElement),
		handler:   handler,
	}
	channel := make(chan *models.Order, 200)
	go ob.runOrderBook(ctx, channel)

	return channel
}

func (ob *orderBook) runOrderBook(ctx context.Context, orderChan chan *models.Order) {

	for {
		select {
		case req := <-orderChan:
			req.OnNewOrderReq(ob.handler)

			switch req.ReqType {

			case models.NewOrder:
				switch req.Side {
				case models.SellOrder:
					ob.addSellRequest(req)
				case models.BuyOrder:
					ob.addBuyRequest(req)
				default:
					log.Printf("Unknown order[%s] side %s", req.ClientID, req.Side)
					continue
				}

			case models.CancelOrder:
				ob.cancelOrder(req)

			default:
				req.ExecuteReject()
				log.Printf("Unknown order[%s] type %s", req.ClientID, req.ReqType)
			}

			if err := ob.handler.PublishExecutions(); err != nil {
				log.Printf("Error publishing executions: %v", err)
			}

		case <-ctx.Done():
			return
		}
	}
}

func (ob *orderBook) addBuyRequest(order *models.Order) {
	ob.processRequest(order, models.Greater)
}

func (ob *orderBook) addSellRequest(order *models.Order) {
	ob.processRequest(order, models.Lesser)
}

func (ob *orderBook) processRequest(order *models.Order, returnCmp models.Comparator) {
	order.ExecuteNew()

	if err := ob.validateReqInOB(order); err != nil {
		order.ExecuteReject()
		return
	}

	ob.matchOrder(order, returnCmp)

	if order.OrdType == models.LimitOrder && (order.Status == models.PartiallyFilled || order.Status == models.NewOrderState) {
		ob.addToOrderBook(order)
	}

	return
}

func (ob *orderBook) cancelOrder(order *models.Order) {

	order.ExecuteCancelReq()
	if err := ob.removeOrder(order); err != nil {
		order.ExecuteReject()
		return
	}
	order.ExecuteCancel()
}

func (ob *orderBook) addToOrderBook(order *models.Order) {

	priceMap, priceList := ob.getOBContainers(order.Side)

	if priceMap[order.Price] == nil {
		priceMap[order.Price] = NewOrderList()
		priceList.Put(order.Price, true)
	}

	priceRef := priceMap[order.Price]

	element := priceRef.Push(order)

	ob.clientIDs[order.ClientID] = element
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

	orderListForPrice := ordersByPrice[price]

	e := orderListForPrice.Front()

	for e != nil && order.Status != models.Filled {

		bookOrder := e.Value()

		tradeQty := 0

		if bookOrder.Quantity <= order.AvailableQty {
			filledOrders = append(filledOrders, bookOrder) //only add orders in order-book as filled orders
			tradeQty = bookOrder.Quantity
		} else {
			tradeQty = order.AvailableQty
		}
		ob.executeTrade(order, bookOrder, tradeQty, price)

		e = e.Next()
	}

	return filledOrders
}

func (ob *orderBook) validateReqInOB(order *models.Order) error {

	if err := order.ValidateReq(); err != nil {
		return err
	}

	if order.ReqType != models.CancelOrder {
		if _, exists := ob.clientIDs[order.ClientID]; exists {
			return errors.New("duplicate order id")
		}
	}
	return nil
}

func (ob *orderBook) removeOrder(order *models.Order) error {

	orderInfo, ok := ob.clientIDs[order.ClientID]
	if !ok {
		return fmt.Errorf("order %s not found in order-book", order.ClientID)
	}

	orderList, priceList := ob.getOBContainers(orderInfo.Value().Side)

	price := orderInfo.Value().Price
	bucket := orderList[price]
	bucket.Remove(orderInfo)

	if bucket.Len() == 0 {
		delete(orderList, price)
		priceList.Remove(price)
	}

	delete(ob.clientIDs, order.ClientID)

	return nil
}

func (ob *orderBook) executeTrade(order *models.Order, bookOrder *models.Order, tradeQty int, price float64) {
	bookOrder.ExecuteTrade(tradeQty, price)
	order.ExecuteTrade(tradeQty, price)

	//ToDo publish Trades for MarketData
}

func (ob *orderBook) getOBContainers(side models.OrderSide) (PriceToOrderMap, *rbt.Tree) {
	switch side {
	case models.BuyOrder:
		return ob.buyOrderContainers.getContainers()
	case models.SellOrder:
		return ob.sellOrderContainers.getContainers()

	default:
		return nil, nil
	}
}
