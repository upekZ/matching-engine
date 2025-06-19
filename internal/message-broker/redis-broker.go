package message_broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/upekZ/matching-engine/internal/models"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"os"
)

type Client struct {
	client *redis.Client
}

func NewMessageBroker() (*Client, error) {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{Addr: redisAddr})
	if _, err := client.Ping(context.Background()).Result(); err != nil {
		return nil, err
	}
	return &Client{client: client}, nil
}

func (c *Client) PublishOrderResponse(ctx context.Context, market string, execReport models.ExecutionReport) error {

	data, err := json.Marshal(execReport)
	if err != nil {
		log.Printf("Error marshalling response: %v", err)
		return fmt.Errorf("failed to publish execution reports")
	}
	if err := c.client.Publish(ctx, "order_responses:"+market, data).Err(); err != nil {
		return err
	}
	return nil
}

func (c *Client) SubscribeToResponsesByBroker(ctx context.Context, market string, responseChannel chan<- models.ExecutionReport) error {
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
