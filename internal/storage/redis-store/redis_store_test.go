package redis_store

import (
	"context"
	"encoding/json"
	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/upekZ/matching-engine/internal/models"
	"os"
	"testing"
	"time"
)

func setupTestRedis(t *testing.T) (*miniredis.Miniredis, *Client) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	origAddr := os.Getenv("REDIS_ADDR")
	os.Setenv("REDIS_ADDR", s.Addr())
	client, err := New()
	if err != nil {
		s.Close()
		t.Fatalf("Failed to create Redis client: %v", err)
	}

	if _, err := client.client.Ping(context.Background()).Result(); err != nil {
		s.Close()
		t.Fatalf("Failed to ping miniredis: %v", err)
	}

	defer os.Setenv("REDIS_ADDR", origAddr)
	return s, client
}

func TestNewClient(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}
	defer s.Close()

	origAddr := os.Getenv("REDIS_ADDR")
	os.Setenv("REDIS_ADDR", s.Addr())
	defer os.Setenv("REDIS_ADDR", origAddr)

	client, err := New()
	assert.NoError(t, err)
	assert.NotNil(t, client.client)

	_, err = client.client.Ping(context.Background()).Result()
	assert.NoError(t, err)
}

func TestSaveExecution(t *testing.T) {
	s, client := setupTestRedis(t)
	defer s.Close()

	exec := &models.Execution{
		ExecID:    "exec1",
		ClOrdID:   "order1",
		ExecType:  models.ExecuteFill,
		OrdStatus: models.Filled,
	}

	err := client.SaveExecution(exec)
	assert.NoError(t, err)

	val, err := s.Get("execution:exec1")
	assert.NoError(t, err)
	assert.NotEmpty(t, val)

	var retrievedExec models.Execution
	err = json.Unmarshal([]byte(val), &retrievedExec)
	assert.NoError(t, err)
	assert.Equal(t, exec.ExecID, retrievedExec.ExecID)
	assert.Equal(t, exec.ClOrdID, retrievedExec.ClOrdID)
}

func TestGetExecutions(t *testing.T) {
	s, client := setupTestRedis(t)
	defer s.Close()

	ctx := context.Background()
	exec1 := &models.Execution{ExecID: "exec1", ClOrdID: "order1"}
	exec2 := &models.Execution{ExecID: "exec2", ClOrdID: "order2"}

	err := client.SaveExecution(exec1)
	assert.NoError(t, err)
	err = client.SaveExecution(exec2)
	assert.NoError(t, err)

	executions, keys, err := client.GetExecutions(ctx)
	assert.NoError(t, err)
	assert.Len(t, executions, 2)
	assert.Len(t, keys, 2)
	assert.Contains(t, keys, "execution:exec1")
	assert.Contains(t, keys, "execution:exec2")
}

func TestClearStoredExecutions(t *testing.T) {
	s, client := setupTestRedis(t)
	defer s.Close()

	ctx := context.Background()
	exec1 := &models.Execution{ExecID: "exec1"}
	exec2 := &models.Execution{ExecID: "exec2"}

	err := client.SaveExecution(exec1)
	assert.NoError(t, err)
	err = client.SaveExecution(exec2)
	assert.NoError(t, err)

	_, keys, err := client.GetExecutions(ctx)
	assert.NoError(t, err)
	assert.Len(t, keys, 2)

	err = client.ClearStoredExecutions(ctx, keys)
	assert.NoError(t, err)

	_, newKeys, err := client.GetExecutions(ctx)
	assert.NoError(t, err)
	assert.Empty(t, newKeys)
}

func TestSaveTrade(t *testing.T) {
	s, client := setupTestRedis(t)
	defer s.Close()

	trade := &models.TradeReport{
		ExecID:        "exec1",
		TradeReportID: "trade1",
		Symbol:        "BTC-USD",
		LastQty:       10,
		LastPx:        50000.0,
	}

	err := client.SaveTrade(trade)
	assert.NoError(t, err)

	val, err := s.Get("execution:exec1")
	assert.NoError(t, err)
	assert.NotEmpty(t, val)

	var retrievedTrade models.TradeReport
	err = json.Unmarshal([]byte(val), &retrievedTrade)
	assert.NoError(t, err)
	assert.Equal(t, trade.ExecID, retrievedTrade.ExecID)
	assert.Equal(t, trade.TradeReportID, retrievedTrade.TradeReportID)
}
