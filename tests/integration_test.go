package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/go-chi/chi/v5"
	_ "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	_ "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/upekZ/matching-engine/internal/api/rest"
	"github.com/upekZ/matching-engine/internal/engine"
	messagebroker "github.com/upekZ/matching-engine/internal/message-broker"
	"github.com/upekZ/matching-engine/internal/models"
	dbwriter "github.com/upekZ/matching-engine/internal/storage/db-writer"
	redisstore "github.com/upekZ/matching-engine/internal/storage/redis-store"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type testSetup struct {
	redisContainer *redis.RedisContainer
	pgContainer    *postgres.PostgresContainer
	redisClient    *redisstore.Client
	msgBroker      *messagebroker.Client
	engine         *engine.Engine
	server         *rest.Server
	router         *chi.Mux
}

func setupTest(t *testing.T) (*testSetup, func()) {
	ctx := context.Background()
	redisContainer, err := redis.Run(ctx, "redis:7.0")
	if err != nil {
		t.Fatalf("failed to start redis container: %v", err)
	}
	redisAddr, err := redisContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get redis connection string: %v", err)
	}

	pgContainer, err := postgres.Run(ctx, "postgres:15",
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	redisClient, err := redisstore.NewCacheClient(redisAddr)
	if err != nil {
		t.Fatalf("failed to create redis client: %v", err)
	}

	msgBroker, err := messagebroker.NewMessageBroker(redisAddr)
	if err != nil {
		t.Fatalf("failed to create message broker: %v", err)
	}

	eng := engine.New(msgBroker, redisClient)

	server := rest.NewServer(eng)
	go server.Start()

	err = dbwriter.RunDBEngine(ctx, redisClient, 1000, 100)
	if err != nil {
		t.Fatalf("failed to start db engine: %v", err)
	}

	cleanup := func() {
		redisContainer.Terminate(ctx)
		pgContainer.Terminate(ctx)
	}

	return &testSetup{
		redisContainer: redisContainer,
		pgContainer:    pgContainer,
		redisClient:    redisClient,
		msgBroker:      msgBroker,
		engine:         eng,
		server:         server,
	}, cleanup
}

func TestIntegration_NewOrder(t *testing.T) {
	setup, cleanup := setupTest(t)
	defer cleanup()

	newOrder := models.Order{
		ClientID: "client1",
		Symbol:   "BTC-USD",
		Side:     models.BuyOrder,
		Price:    50000.0,
		Quantity: 1,
		ReqType:  models.NewLimitOrder,
	}

	body, _ := json.Marshal(newOrder)
	req, _ := http.NewRequest("POST", "/orders", bytes.NewBuffer(body))
	rr := httptest.NewRecorder()

	setup.router.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	responseChannel := make(chan models.ExecutionReport, 100)
	err := setup.engine.SubscribeToResponses(ctx, "BTCUSD", responseChannel)
	assert.NoError(t, err)

	select {
	case report := <-responseChannel:
		assert.Contains(t, report, newOrder.ClientID)
		executions := report[newOrder.ClientID]
		assert.GreaterOrEqual(t, len(executions), 1)
		for _, exec := range executions {
			assert.Equal(t, newOrder.ClientID, exec.ClOrdID)
			assert.Equal(t, newOrder.Symbol, exec.Symbol)
			assert.Equal(t, models.ExecuteNew, exec.ExecType)
			assert.Equal(t, models.NewOrderState, exec.OrdStatus)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for execution report")
	}
}

func TestIntegration_OrderMatching(t *testing.T) {
	setup, cleanup := setupTest(t)
	defer cleanup()

	buyOrder := models.Order{
		ClientID: "client1",
		Symbol:   "BTC-USD",
		Side:     models.BuyOrder,
		Price:    50000.0,
		Quantity: 1,
		ReqType:  models.NewLimitOrder,
	}

	body, _ := json.Marshal(buyOrder)
	req, _ := http.NewRequest("POST", "/orders", bytes.NewBuffer(body))
	rr := httptest.NewRecorder()
	setup.router.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	sellOrder := models.Order{
		ClientID: "client2",
		Symbol:   "BTC-USD",
		Side:     models.SellOrder,
		Price:    50000.0,
		Quantity: 1,
		ReqType:  models.NewLimitOrder,
	}

	body, _ = json.Marshal(sellOrder)
	req, _ = http.NewRequest("POST", "/orders", bytes.NewBuffer(body))
	rr = httptest.NewRecorder()
	setup.router.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	responseChannel := make(chan models.ExecutionReport, 100)
	err := setup.engine.SubscribeToResponses(ctx, "BTC-USD", responseChannel)
	assert.NoError(t, err)

	reportsReceived := 0
	for reportsReceived < 2 {
		select {
		case report := <-responseChannel:
			reportsReceived++
			for clientID, executions := range report {
				for _, exec := range executions {
					if exec.ExecType == models.ExecuteTrade {
						assert.Equal(t, 1, exec.LastQty)
						assert.Equal(t, 50000.0, exec.LastPx)
						if clientID == "client1" {
							assert.Equal(t, models.BuyOrder, exec.Side)
						} else if clientID == "client2" {
							assert.Equal(t, models.SellOrder, exec.Side)
						}
						assert.Equal(t, models.Filled, exec.OrdStatus)
					}
				}
			}
		case <-ctx.Done():
			t.Fatalf("timeout waiting for execution report, received %d/2", reportsReceived)
		}
	}
}

func TestIntegration_CancelOrder(t *testing.T) {
	setup, cleanup := setupTest(t)
	defer cleanup()

	newOrder := models.Order{
		ClientID: "client1",
		Symbol:   "BTC-USD",
		Side:     models.BuyOrder,
		Price:    50000.0,
		Quantity: 1,
		ReqType:  models.NewLimitOrder,
	}

	body, _ := json.Marshal(newOrder)
	req, _ := http.NewRequest("POST", "/orders", bytes.NewBuffer(body))
	rr := httptest.NewRecorder()
	setup.router.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	cancelOrder := models.Order{
		ClientID: "client1",
		Symbol:   "BTC-USD",
		Side:     models.BuyOrder,
		ReqType:  models.CancelOrder,
	}

	body, _ = json.Marshal(cancelOrder)
	req, _ = http.NewRequest("DELETE", "/orders", bytes.NewBuffer(body))
	rr = httptest.NewRecorder()
	setup.router.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	responseChannel := make(chan models.ExecutionReport, 100)
	err := setup.engine.SubscribeToResponses(ctx, "BTC-USD", responseChannel)
	assert.NoError(t, err)

	select {
	case report := <-responseChannel:
		assert.Contains(t, report, newOrder.ClientID)
		executions := report[newOrder.ClientID]
		assert.GreaterOrEqual(t, len(executions), 1)
		for _, exec := range executions {
			if exec.ExecType == models.ExecuteCancel {
				assert.Equal(t, newOrder.ClientID, exec.ClOrdID)
				assert.Equal(t, newOrder.Symbol, exec.Symbol)
				assert.Equal(t, models.Cancelled, exec.OrdStatus)
			}
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for cancel execution report")
	}
}

func TestIntegration_ExecutionPersistence(t *testing.T) {
	setup, cleanup := setupTest(t)
	defer cleanup()

	buyOrder := models.Order{
		ClientID: "client1",
		Symbol:   "BTC-USD",
		Side:     models.BuyOrder,
		Price:    50000.0,
		Quantity: 1,
		ReqType:  models.NewLimitOrder,
	}

	body, _ := json.Marshal(buyOrder)
	req, _ := http.NewRequest("POST", "/orders", bytes.NewBuffer(body))
	rr := httptest.NewRecorder()
	setup.router.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	time.Sleep(6 * time.Second) // Wait longer than ticker duration in db_writer

	executions, _, err := setup.redisClient.GetExecutions(context.Background())
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(executions), 1)
	found := false
	for _, exec := range executions {
		if exec.ClOrdID == "client1" && exec.ExecType == models.ExecuteNew {
			found = true
			assert.Equal(t, "BTC-USD", exec.Symbol)
			assert.Equal(t, models.NewOrderState, exec.OrdStatus)
		}
	}
	assert.True(t, found, "expected execution not found in cache")
}
