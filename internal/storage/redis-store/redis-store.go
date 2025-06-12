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

func (c *Client) SaveTrades(trades *models.Execution) error {

	data, err := json.Marshal(trades)
	if err != nil {
		return err
	}

	if err := c.client.Set(context.Background(), "execution:"+trades.ExecID, data, 0).Err(); err != nil {
		return err
	}
	return nil
}

func (c *Client) GetExecutions(ctx context.Context) ([]*models.Execution, []string, error) {

	keys, err := c.client.Keys(ctx, "execution:*").Result()
	if err != nil {
		log.Printf("error getting keys: %v", err)
		return nil, nil, err
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
	return trades, keys, nil
}

func (c *Client) ClearCachedExecutions(ctx context.Context, keys []string) error {

	_, err := c.client.Del(ctx, keys...).Result()
	if err != nil {
		log.Printf("error deleting keys: %v", err)
		return err
	}

	return nil
}
