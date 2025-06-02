package redisstore

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
	"github.com/upekZ/matching-engine/internal/models"
)

type Serializable interface {
	ToJSON() ([]byte, error)
}
type Client struct {
	client *redis.Client
}

func NewClient(addr string) (*Client, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})
	if _, err := client.Ping(context.Background()).Result(); err != nil {
		return nil, err
	}
	return &Client{client: client}, nil
}

func (c *Client) SaveOrderBook(market string, obj Serializable, keyPrefix string) error {
	data, err := obj.ToJSON()
	if err != nil {
		return err
	}
	key := keyPrefix + ":" + market
	return c.client.Set(context.Background(), key, data, 0).Err()
}

func (c *Client) SaveTrades(market string, trades *models.TradeManager) error {

	var allTrades [][]byte

	for _, trade := range trades.GetTrades() {
		data, err := json.Marshal(trade)
		if err != nil {
			return err
		}

		if err := c.client.Set(context.Background(), "trade:"+trade.GetID(), data, 0).Err(); err != nil {
			return err
		}
		allTrades = append(allTrades, data)
	}

	payload, err := json.Marshal(allTrades)
	if err != nil {
		return err
	}

	return c.client.Publish(context.Background(), "trades:"+market, payload).Err()

}

func (c *Client) PublishOrderResponse(ctx context.Context, market string, data []byte) error {
	return c.client.Publish(ctx, "order_responses:"+market, data).Err()
}
