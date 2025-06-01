package engine

import (
	"fmt"
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
	"sync"
)

type PriceInfo struct {
	volume    int
	orderList *OrderList

	mu sync.Mutex
}

func NewPriceInfo() *PriceInfo {
	return &PriceInfo{
		volume:    0,
		orderList: NewOrderList(),
		mu:        sync.Mutex{},
	}
}

func (info *PriceInfo) Remove(order *OrderElement) {
	info.mu.Lock()
	defer info.mu.Unlock()

	info.orderList.Remove(order)
	info.volume -= order.Value().GetQty()
}

type OrderBook struct {
	market            string
	SellOrdersByPrice map[float64]*PriceInfo
	BuyOrdersByPrice  map[float64]*PriceInfo
	SellPrices        *rbt.Tree
	BuyPrices         *rbt.Tree
	OrderIndex        map[string]*OrderElement
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
		SellOrdersByPrice: make(map[float64]*PriceInfo),
		BuyOrdersByPrice:  make(map[float64]*PriceInfo),
		SellPrices:        rbt.NewWith(sellComparator),
		BuyPrices:         rbt.NewWith(buyComparator),
		OrderIndex:        make(map[string]*OrderElement),
	}
}

func (ob *OrderBook) AddSellOrder(order *models.Order) {

	//ob.MatchSellOrder(order)

	if ob.SellOrdersByPrice[order.GetPrice()] == nil {
		ob.SellOrdersByPrice[order.GetPrice()] = NewPriceInfo()
		ob.SellPrices.Put(order.GetPrice(), true)
	}
	priceRef := ob.SellOrdersByPrice[order.GetPrice()]

	priceRef.volume += order.GetQty()
	element := priceRef.orderList.Push(order)

	ob.OrderIndex[order.GetID()] = element
}

func (ob *OrderBook) AddBuyOrder(order *models.Order) ([]*models.Trade, error) {

	trade, err := ob.matchBuyOrder(order)

	if err != nil {
		return nil, err
	}

	if order.GetQty() == 0 {
		return trade, nil
	}

	if ob.SellOrdersByPrice[order.GetPrice()] == nil {
		ob.SellOrdersByPrice[order.GetPrice()] = NewPriceInfo()
		ob.SellPrices.Put(order.GetPrice(), true)
	}
	priceRef := ob.SellOrdersByPrice[order.GetPrice()]

	priceRef.volume += order.GetQty()
	element := priceRef.orderList.Push(order)

	ob.OrderIndex[order.GetID()] = element

	return trade, nil
}

func (ob *OrderBook) CancelOrder(orderID string) bool {
	orderInfo, exists := ob.OrderIndex[orderID]
	if !exists {
		return false
	}

	price := orderInfo.Value().GetPrice()
	bucket := ob.SellOrdersByPrice[price]
	bucket.Remove(orderInfo)

	if bucket.volume == 0 {
		delete(ob.SellOrdersByPrice, price)
		ob.SellPrices.Remove(price)
	}

	delete(ob.OrderIndex, orderID)
	return true
}

func (ob *OrderBook) matchBuyOrder(buyOrder *models.Order) ([]*models.Trade, error) {

	reqQuantity := buyOrder.GetQty()
	buyPrice := buyOrder.GetPrice()

	var wg sync.WaitGroup
	var toDelete []float64

	trades := make([]*models.Trade, 0)
	tradeChan := make(chan []*models.Trade, 264)

	defer func() {
		wg.Wait()
		close(tradeChan)
		for item := range tradeChan {
			trades = append(trades, item...)
		}
		for _, id := range toDelete {
			ob.SellPrices.Remove(id)
		}
	}()

	for _, key := range ob.SellPrices.Keys() {

		price := key.(float64)
		if price > buyPrice {
			return trades, nil
		}

		bucket := ob.SellOrdersByPrice[price]

		reqVolFromBucket := 0
		if reqQuantity > bucket.volume {
			reqVolFromBucket = bucket.volume
			reqQuantity -= bucket.volume
			_ = append(toDelete, price)
		} else {
			reqVolFromBucket = reqQuantity
			reqQuantity = 0
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			priceTrades, err := ob.matchOrdersInPrice(price, buyOrder, reqVolFromBucket)
			if err != nil {
				log.Printf("matching err: %v", err)
			}

			tradeChan <- priceTrades
		}()

		if reqQuantity == 0 {
			break
		}

	}
	return trades, nil
}

func (ob *OrderBook) matchOrdersInPrice(price float64, buyOrder *models.Order, reqVolFromPrice int) ([]*models.Trade, error) {

	priceInfo := ob.SellOrdersByPrice[price]
	trades := make([]*models.Trade, 0)

	e := priceInfo.orderList.Front()

	for e != nil {

		sellOrder := e.Value()
		if sellOrder.GetQty() <= reqVolFromPrice {
			if !buyOrder.ReduceQuantity(sellOrder.GetQty()) {
				fmt.Println("conflicting sell order")
				return trades, fmt.Errorf("conflicting sell order")
			}
			reqVolFromPrice -= sellOrder.GetQty()
			delete(ob.OrderIndex, sellOrder.GetID())

			priceInfo.orderList.Pop()
			e = priceInfo.orderList.Front()

			trades = append(trades, models.NewTrade(buyOrder.GetID(), sellOrder.GetID(), sellOrder.GetPrice(), sellOrder.GetQty()))

			if e == nil {
				delete(ob.SellOrdersByPrice, price)
				ob.SellPrices.Remove(price)
			}

		} else {
			sellOrder.ReduceQuantity(reqVolFromPrice)
			buyOrder.ReduceQuantity(reqVolFromPrice)
			trades = append(trades, models.NewTrade(buyOrder.GetID(), sellOrder.GetID(), sellOrder.GetPrice(), reqVolFromPrice))
			return trades, nil
		}
	}

	return trades, nil
}

func (ob *OrderBook) removeOrder(order *models.Order) {
	delete(ob.OrderIndex, order.GetID())
}

func (ob *OrderBook) matchSellOrder(sellOrder *models.Order) int {
	reqQuantity := sellOrder.GetQty()
	sellPrice := sellOrder.GetPrice()

	var wg sync.WaitGroup

	for sellOrder.GetQty() > 0 {
		highestNode := ob.BuyPrices.Right()
		if highestNode == nil {
			break
		}

		price := highestNode.Key.(float64)
		if price < sellPrice {
			return reqQuantity
		}

		bucket := ob.BuyOrdersByPrice[price]

		if reqQuantity > bucket.volume {
			reqQuantity -= bucket.volume

			wg.Add(1)
			go func() {
				defer wg.Done()
				for e := bucket.orderList.Front(); e != nil; e = e.Next() {
					buyOrder := e.Value()
					fmt.Printf("Matched %d with %d: %d units at %.2f\n", sellOrder.GetID(), buyOrder.GetID(), buyOrder.GetQty(), price)
					sellOrder.ReduceQuantity(buyOrder.GetQty())
					bucket.Remove(ob.OrderIndex[buyOrder.GetID()])
					delete(ob.OrderIndex, buyOrder.GetID())
				}
				delete(ob.BuyOrdersByPrice, price)
				ob.BuyPrices.Remove(price)
			}()
		} else {
			wg.Wait()
			for e := bucket.orderList.Front(); e != nil; e = e.Next() {
				buyOrder := e.Value()
				if reqQuantity < buyOrder.GetQty() {
					buyOrder.ReduceQuantity(sellOrder.GetQty())
					fmt.Printf("Matched %d with %d: %d units at %.2f\n", sellOrder.GetID(), buyOrder.GetID(), sellOrder.GetQty(), price)
					wg.Wait()
					return 0
				}
				fmt.Printf("Matched %d with %d: %d units at %.2f\n", sellOrder.GetID(), buyOrder.GetID(), buyOrder.GetQty(), price)
				sellOrder.ReduceQuantity(buyOrder.GetQty())
				bucket.Remove(ob.OrderIndex[buyOrder.GetID()])
				delete(ob.OrderIndex, buyOrder.GetID())
			}
		}
	}

	wg.Wait()

	return sellOrder.GetQty()
}

func (ob *OrderBook) executeBuyTrade(buyOrder *models.Order, sellOrder *models.Order) *models.Trade {

	if buyOrder.GetQty() > sellOrder.GetQty() {
		buyOrder.ReduceQuantity(sellOrder.GetQty())
		return models.NewTrade(buyOrder.GetID(), sellOrder.GetID(), sellOrder.GetPrice(), sellOrder.GetQty())
	} else {
		return models.NewTrade(buyOrder.GetID(), sellOrder.GetID(), sellOrder.GetPrice(), buyOrder.GetQty())
	}
}
