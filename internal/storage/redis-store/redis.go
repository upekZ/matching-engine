package redis_store

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/redis/go-redis/v9"
	"github.com/upekZ/matching-engine/internal/models"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
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

func (c *Client) SaveOrderBook(market string, obj Serializable, keyPrefix string) error { //ToDo plug order-book saving to main
	data, err := obj.ToJSON()
	if err != nil {
		return err
	}
	key := keyPrefix + ":" + market
	return c.client.Set(context.Background(), key, data, 0).Err()
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

func (c *Client) PublishOrderResponse(ctx context.Context, market string, data []byte) error {
	if err := c.client.Publish(ctx, "order_responses:"+market, data).Err(); err != nil {
		return err
	}
	return nil
}

func (c *Client) SubscribeToResponses(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error {
	if market == "" {
		return status.Error(codes.InvalidArgument, "Symbol must be specified")
	}

	defer func() {
		close(responseChannel)
	}()

	pubSub := c.client.Subscribe(ctx, "order_responses:"+market)
	defer pubSub.Close()

	for {
		msg, err := pubSub.ReceiveMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil // Client disconnected
			}
			return status.Errorf(codes.Internal, "Failed to receive message: %v", err)
		}

		var modelResp models.ExecutionReport
		if err := json.Unmarshal([]byte(msg.Payload), &modelResp); err != nil {
			log.Printf("Failed to unmarshal response: %v", err)
			continue
		}

		responseChannel <- modelResp
	}
}
