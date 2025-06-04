package engine

import (
	"encoding/json"
	"fmt"
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
)

type SellOrders struct {
	SellOrdersByPrice map[float64]*models.OrderList
	SellPrices        *rbt.Tree
}

type BuyOrders struct {
	BuyOrdersByPrice map[float64]*models.OrderList
	BuyPrices        *rbt.Tree
}

type CourseContainer interface {
	SellOrders | BuyOrders
}

type OrderBook struct {
	market              string
	SellOrderContainers SellOrders
	BuyOrderContainers  BuyOrders
	OrderIndex          map[string]*models.OrderElement
	ClientIDs           map[string]string
}

func NewOrderBook(market string) *OrderBook {
	sellComparator := func(a, b interface{}) int {
		aPrice, bPrice := a.(float64), b.(float64)
		if aPrice < bPrice {
			return -1
		}
		if aPrice > bPrice {
			return 1
		}
		return 0
	}

	buyComparator := func(a, b interface{}) int {
		aPrice, bPrice := a.(float64), b.(float64)
		if aPrice < bPrice {
			return -1
		}
		if aPrice > bPrice {
			return 1
		}
		return 0
	}

	return &OrderBook{
		market: market,

		SellOrderContainers: SellOrders{
			SellOrdersByPrice: make(map[float64]*models.OrderList),
			SellPrices:        rbt.NewWith(sellComparator),
		},
		BuyOrderContainers: BuyOrders{
			BuyOrdersByPrice: make(map[float64]*models.OrderList),
			BuyPrices:        rbt.NewWith(buyComparator),
		},

		OrderIndex: make(map[string]*models.OrderElement),
		ClientIDs:  make(map[string]string),
	}
}

func (ob *OrderBook) AddBuyOrder(order *models.Order) ([]*models.Trade, error) {

	trades, err := ob.matchOrder(order, models.Less)

	if err != nil {
		return nil, err
	}

	if order.Status != models.Filled {

		if err := ob.addToOrderBook(order); err != nil {
			log.Printf("Error adding order to orderbook: %v", err)
			return trades, fmt.Errorf("order request partially executed")
		}
	}
	return trades, nil
}

func (ob *OrderBook) AddSellOrder(order *models.Order) ([]*models.Trade, error) {

	trades, err := ob.matchOrder(order, models.Greater)

	if err != nil {
		return nil, err
	}

	if order.Status != models.Filled {

		if err := ob.addToOrderBook(order); err != nil {
			log.Printf("Error adding order to orderbook: %v", err)
			return trades, fmt.Errorf("order request partially executed")
		}
	}
	return trades, nil
}

func (ob *OrderBook) addToOrderBook(order *models.Order) error {

	var priceRef *models.OrderList

	switch order.Side {
	case models.SellOrder:
		if ob.SellOrderContainers.SellOrdersByPrice[order.Price] == nil {
			ob.SellOrderContainers.SellOrdersByPrice[order.Price] = models.NewOrderList()
			ob.SellOrderContainers.SellPrices.Put(order.Price, true)
		}
		priceRef = ob.SellOrderContainers.SellOrdersByPrice[order.Price]
	case models.BuyOrder:
		if ob.BuyOrderContainers.BuyOrdersByPrice[order.Price] == nil {
			ob.BuyOrderContainers.BuyOrdersByPrice[order.Price] = models.NewOrderList()
			ob.BuyOrderContainers.BuyPrices.Put(order.Price, true)
		}
		priceRef = ob.BuyOrderContainers.BuyOrdersByPrice[order.Price]
	default:
		return fmt.Errorf("order: %s not added to order-book invalid order type", order.ID)
	}

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

	var orderList map[float64]*models.OrderList
	var priceList *rbt.Tree

	switch order.Side {
	case models.SellOrder:
		orderList = ob.SellOrderContainers.SellOrdersByPrice
		priceList = ob.SellOrderContainers.SellPrices
	case models.BuyOrder:
		orderList = ob.BuyOrderContainers.BuyOrdersByPrice
		priceList = ob.BuyOrderContainers.BuyPrices
	default:
		return nil, fmt.Errorf("order: %s not added to order-book invalid order type", order.ID)
	}

	price := orderInfo.Value().Price
	bucket := orderList[price]
	bucket.Remove(orderInfo)

	if bucket.Len() == 0 {
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

	var keys []interface{}

	switch order.Side {
	case models.BuyOrder:
		keys = ob.SellOrderContainers.SellPrices.Keys()
	case models.SellOrder:
		keys = ob.BuyOrderContainers.BuyPrices.Keys()
	default:
		return nil, fmt.Errorf("order: %s not added to order-book invalid order type", order.ID)
	}

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

	var ordersByPrice map[float64]*models.OrderList

	switch order.Side {
	case models.BuyOrder:
		ordersByPrice = ob.SellOrderContainers.SellOrdersByPrice
	case models.SellOrder:
		ordersByPrice = ob.BuyOrderContainers.BuyOrdersByPrice
	default:
		return nil, fmt.Errorf("order matching failed\n")
	}
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

	var orderList map[float64]*models.OrderList
	var priceList *rbt.Tree

	switch order.Side {
	case models.SellOrder:
		orderList = ob.SellOrderContainers.SellOrdersByPrice
		priceList = ob.SellOrderContainers.SellPrices
	case models.BuyOrder:
		orderList = ob.BuyOrderContainers.BuyOrdersByPrice
		priceList = ob.BuyOrderContainers.BuyPrices
	default:
		return fmt.Errorf("order: %s not added to order-book invalid order type", order.ID)
	}

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
