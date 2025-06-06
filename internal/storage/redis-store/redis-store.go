package redis_store

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
)

type Client struct {
	client *redis.Client
}

func NewCacheClient(addr string) (*Client, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})
	if _, err := client.Ping(context.Background()).Result(); err != nil {
		return nil, err
	}
	return &Client{client: client}, nil
}

func (c *Client) SaveTrades(market string, trades []*models.Execution) error {

	var allTrades [][]byte

	for _, trade := range trades {
		data, err := json.Marshal(trade)
		if err != nil {
			return err
		}

		if err := c.client.Set(context.Background(), "trade:"+trade.ExecID, data, 0).Err(); err != nil {
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

func (c *Client) GetExecutions(ctx context.Context) ([]*models.Execution, error) {

	keys, err := c.client.Keys(ctx, "trade:*").Result()
	if err != nil {
		log.Printf("error getting keys: %v", err)
		return nil, err
	}

	var trades []*models.Execution
	for _, key := range keys {
		val, err := c.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		var exec models.Execution
		if err := json.Unmarshal([]byte(val), &exec); err != nil {
			continue
		}
		trades = append(trades, &exec)
	}
	return trades, nil
}
