package engine

import (
	"encoding/json"
	"fmt"
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/google/uuid"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
	"sync"
	"time"
)

type OrderBook struct {
	market            string
	SellOrdersByPrice map[float64]*PriceInfo
	BuyOrdersByPrice  map[float64]*PriceInfo
	SellPrices        *rbt.Tree
	BuyPrices         *rbt.Tree
	OrderIndex        map[string]*models.OrderElement
	ClientIDs         map[string]string
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
		market:            market,
		SellOrdersByPrice: make(map[float64]*PriceInfo),
		BuyOrdersByPrice:  make(map[float64]*PriceInfo),
		SellPrices:        rbt.NewWith(sellComparator),
		BuyPrices:         rbt.NewWith(buyComparator),
		OrderIndex:        make(map[string]*models.OrderElement),
		ClientIDs:         make(map[string]string),
	}
}

func (ob *OrderBook) AddBuyOrder(order *models.Order) (*models.TradeManager, error) {

	trades, err := ob.matchOrder(order, models.Less)

	if err != nil {
		return nil, err
	}

	if trades.GetVolume() < order.Quantity {

		order.Quantity -= trades.GetVolume()
		if err := ob.addToOrderBook(order); err != nil {
			log.Printf("Error adding order to orderbook: %v", err)
			return trades, fmt.Errorf("order request partially executed")
		}
	}
	return trades, nil
}

func (ob *OrderBook) AddSellOrder(order *models.Order) (*models.TradeManager, error) {

	trades, err := ob.matchOrder(order, models.Greater)

	if err != nil {
		return nil, err
	}

	if trades.GetVolume() < order.Quantity {

		order.Quantity -= trades.GetVolume()
		if err := ob.addToOrderBook(order); err != nil {
			log.Printf("Error adding order to orderbook: %v", err)
			return trades, fmt.Errorf("order request partially executed")
		}
	}
	return trades, nil
}

func (ob *OrderBook) addToOrderBook(order *models.Order) error {

	var priceRef *PriceInfo

	switch order.Side {
	case models.SellOrder:
		if ob.SellOrdersByPrice[order.Price] == nil {
			ob.SellOrdersByPrice[order.Price] = NewPriceInfo()
			ob.SellPrices.Put(order.Price, true)
		}
		priceRef = ob.SellOrdersByPrice[order.Price]
	case models.BuyOrder:
		if ob.BuyOrdersByPrice[order.Price] == nil {
			ob.BuyOrdersByPrice[order.Price] = NewPriceInfo()
			ob.BuyPrices.Put(order.Price, true)
		}
		priceRef = ob.BuyOrdersByPrice[order.Price]
	default:
		return fmt.Errorf("order: %s not added to order-book invalid order type", order.ID)
	}

	priceRef.volume += order.Quantity
	element := priceRef.orderList.Push(order)

	ob.OrderIndex[order.ID] = element
	ob.ClientIDs[order.ClientID] = order.ID
	return nil
}

func (ob *OrderBook) CancelOrder(order *models.Order) (*models.TradeManager, error) {

	orderID, ok := ob.ClientIDs[order.ClientID]
	if !ok {
		return nil, fmt.Errorf("order %s not found in order-book", order.ID)
	}

	orderInfo, exists := ob.OrderIndex[orderID]
	if !exists {
		return nil, fmt.Errorf("order: %s doesn't exist", order.ID)
	}

	price := orderInfo.Value().Price
	bucket := ob.SellOrdersByPrice[price]
	bucket.Remove(orderInfo)

	if bucket.volume == 0 {
		delete(ob.SellOrdersByPrice, price)
		ob.SellPrices.Remove(price)
	}

	trades := models.NewTradeManager()
	trades.AddTrade(&models.Trade{
		ID:        order.ID,
		Price:     order.Price,
		Quantity:  order.Quantity,
		Market:    order.Market,
		Status:    "cancelled",
		BuyOrder:  "N/A",
		SellOrder: "N/A",
		Timestamp: time.Now().Unix(),
	})
	//ToDo a different type of trades or to add a field to order
	delete(ob.OrderIndex, order.ID)
	delete(ob.ClientIDs, order.ClientID)
	return trades, nil
}

func (ob *OrderBook) matchOrder(order *models.Order, returnCmp models.Comparator) (*models.TradeManager, error) {
	reqQuantity := order.Quantity
	orderPrice := order.Price

	var wg sync.WaitGroup
	var tradeWg sync.WaitGroup
	var toDeletePrices []float64
	trades := models.NewTradeManager()

	tradeChan := make(chan *models.TradeManager, 64)
	errChan := make(chan error)

	var keys []interface{}
	var ordersByPrice map[float64]*PriceInfo

	switch order.Side {
	case models.BuyOrder:
		keys = ob.SellPrices.Keys()
		ordersByPrice = ob.SellOrdersByPrice
	case models.SellOrder:
		keys = ob.BuyPrices.Keys()
		ordersByPrice = ob.BuyOrdersByPrice
	default:
		return nil, fmt.Errorf("order: %s not added to order-book invalid order type", order.ID)
	}

	tradeWg.Add(1)
	go func() {
		defer tradeWg.Done()
		for item := range tradeChan {
			trades.Append(item)
		}
	}()

	for _, key := range keys {
		existingPrice := key.(float64)
		if !returnCmp(existingPrice, orderPrice) {
			break
		}

		bucketVolume := ordersByPrice[existingPrice].volume

		reqVolFromBucket := 0
		if reqQuantity > bucketVolume {
			reqVolFromBucket = bucketVolume
			reqQuantity -= bucketVolume
			toDeletePrices = append(toDeletePrices, existingPrice) //ToDo modify iterator to delete the existingPrice on the go
		} else {
			reqVolFromBucket = reqQuantity
			reqQuantity = 0
		}

		wg.Add(1)
		go func(price float64, id string, vol int) {
			defer wg.Done()
			priceTrades, err := ob.matchOrdersInPrice(price, id, reqVolFromBucket, order.Side)
			if err != nil {
				errChan <- err
			}
			tradeChan <- priceTrades
		}(existingPrice, order.ID, reqVolFromBucket)

		if reqQuantity == 0 {
			break
		}
	}

	wg.Wait()
	close(tradeChan)
	tradeWg.Wait()

	select {
	case err := <-errChan:
		return nil, err
	default:
	}

	for _, id := range toDeletePrices {
		switch order.Side {
		case models.BuyOrder:
			ob.SellPrices.Remove(id)
		case models.SellOrder:
			ob.BuyPrices.Remove(id)
		}
	}
	return trades, nil
}

func (ob *OrderBook) matchOrdersInPrice(price float64, OrderID string, reqVolFromPrice int, orderType models.OrderType) (*models.TradeManager, error) {

	var ordersByPrice map[float64]*PriceInfo

	switch orderType {
	case models.BuyOrder:
		ordersByPrice = ob.SellOrdersByPrice
	case models.SellOrder:
		ordersByPrice = ob.BuyOrdersByPrice
	default:
		return nil, fmt.Errorf("order matching failed\n")
	}
	priceInfo := ordersByPrice[price]

	e := priceInfo.orderList.Front()

	matchedTrades := models.NewTradeManager()

	for e != nil {

		bookOrder := e.Value()
		if bookOrder.Quantity <= reqVolFromPrice {
			reqVolFromPrice -= bookOrder.Quantity
			delete(ob.OrderIndex, bookOrder.ID)

			priceInfo.orderList.Pop()
			e = priceInfo.orderList.Front()

			matchedTrades.AddTrade(&models.Trade{
				ID:        uuid.New().String(),
				Price:     bookOrder.Price,
				Quantity:  bookOrder.Quantity,
				Market:    bookOrder.Market,
				Status:    "filled",
				BuyOrder:  OrderID,
				SellOrder: bookOrder.ClientID,
				Timestamp: time.Now().Unix(),
			})

			if e == nil {
				delete(ob.SellOrdersByPrice, price)
				ob.SellPrices.Remove(price)
			}

		} else {
			bookOrder.ReduceQuantity(reqVolFromPrice)

			matchedTrades.AddTrade(&models.Trade{
				ID:        uuid.New().String(),
				Price:     bookOrder.Price,
				Quantity:  reqVolFromPrice,
				Market:    bookOrder.Market,
				Status:    "filled",
				BuyOrder:  OrderID,
				SellOrder: bookOrder.ClientID,
				Timestamp: time.Now().Unix(),
			})

			break
		}
	}

	return matchedTrades, nil
}

func (ob *OrderBook) ToJSON() ([]byte, error) {
	return json.Marshal(ob)
}
