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
	orderList *models.OrderList

	mu sync.Mutex
}

func NewPriceInfo() *PriceInfo {
	return &PriceInfo{
		volume:    0,
		orderList: models.NewOrderList(),
		mu:        sync.Mutex{},
	}
}

func (info *PriceInfo) Remove(order *models.OrderElement) {
	info.mu.Lock()
	defer info.mu.Unlock()

	info.orderList.Remove(order)
	info.volume -= order.Value().GetQty()
}

type OrderBook struct {
	SellOrdersByPrice map[float64]*PriceInfo
	BuyOrdersByPrice  map[float64]*PriceInfo
	SellPrices        *rbt.Tree
	BuyPrices         *rbt.Tree
	OrderIndex        map[int]*models.OrderElement
}

func NewOrderBook() *OrderBook {
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
		OrderIndex:        make(map[int]*models.OrderElement),
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

func (ob *OrderBook) AddBuyOrder(order *models.Order) {

	if ob.SellOrdersByPrice[order.GetPrice()] == nil {
		ob.SellOrdersByPrice[order.GetPrice()] = NewPriceInfo()
		ob.SellPrices.Put(order.GetPrice(), true)
	}
	priceRef := ob.SellOrdersByPrice[order.GetPrice()]

	priceRef.volume += order.GetQty()
	element := priceRef.orderList.Push(order)

	ob.OrderIndex[order.GetID()] = element
}

func (ob *OrderBook) CancelOrder(orderID int) bool {
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

func (ob *OrderBook) matchBuyOrder(buyOrder *models.Order) *models.Order {

	reqQuantity := buyOrder.GetQty()
	buyPrice := buyOrder.GetPrice()

	var wg sync.WaitGroup
	var toDelete []float64

	defer func() {
		wg.Wait()
		for _, id := range toDelete {
			ob.SellPrices.Remove(id)
		}
	}()

	for _, key := range ob.SellPrices.Keys() {

		price := key.(float64)
		if price > buyPrice {
			return buyOrder
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
			if err := ob.matchOrdersInPrice(price, buyOrder, reqVolFromBucket); err != nil {
				log.Printf("matching err: %v", err)
			}
			wg.Done()
		}()

		if reqQuantity == 0 {
			return buyOrder
		}

	}

	return buyOrder
}

func (ob *OrderBook) matchOrdersInPrice(price float64, buyOrder *models.Order, reqVol int) error {

	priceInfo := ob.SellOrdersByPrice[price]

	e := priceInfo.orderList.Front()

	for e != nil {

		sellOrder := e.Value()
		if sellOrder.GetQty() <= reqVol {
			if !buyOrder.ReduceQuantity(sellOrder.GetQty()) {
				fmt.Println("conflicting sell order")
				return fmt.Errorf("conflicting sell order")
			}
			reqVol -= sellOrder.GetQty()
			delete(ob.OrderIndex, sellOrder.GetID())

			priceInfo.orderList.Pop()
			e = priceInfo.orderList.Front()

			if e == nil {
				delete(ob.SellOrdersByPrice, price)
				ob.SellPrices.Remove(price)
			}

		} else {
			sellOrder.ReduceQuantity(reqVol)
			buyOrder.ReduceQuantity(reqVol)
			return nil
		}
	}

	return nil
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
