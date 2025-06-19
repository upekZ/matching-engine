package db_writer

import (
	"context"
	_ "github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/upekZ/matching-engine/internal/models"

	"log"
	"time"
)

type CacheClient interface {
	GetExecutions(ctx context.Context) ([]*models.Execution, []string, error)
	ClearCachedExecutions(ctx context.Context, keys []string) error
}

type BDHandler interface {
	InsertExecution(ctx context.Context, exec *models.Execution) error
}

type DbEngine struct {
	executionQueue chan []*models.Execution
	maxBatchSize   int
	batchSize      int
	cacheClient    CacheClient
	ctx            context.Context
	cancel         context.CancelFunc
	dbHandler      BDHandler
}

func RunDBEngine(ctx context.Context, queryGen BDHandler, cacheClient CacheClient, maxBatchSize int, batchSize int) error {
	cancelCtx, cancel := context.WithCancel(ctx)

	engine := &DbEngine{
		executionQueue: make(chan []*models.Execution, batchSize),
		maxBatchSize:   maxBatchSize,
		batchSize:      batchSize,
		cacheClient:    cacheClient,
		ctx:            cancelCtx,
		cancel:         cancel,
		dbHandler:      queryGen,
	}

	go engine.startExecWriter()
	return nil
}

func (e *DbEngine) startExecWriter() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var batch []*models.Execution

	for {
		select {
		case exec, ok := <-e.executionQueue:
			if !ok {
				e.flushExecutions(exec)
				return
			}

			batch = append(batch, exec...)
			if len(batch) >= e.maxBatchSize {
				e.flushExecutions(batch)
				batch = nil
			}

		case <-ticker.C:
			executions, keys, err := e.cacheClient.GetExecutions(context.Background())
			if err == nil && len(keys) > 0 {
				e.executionQueue <- executions
			} else {
				continue
			}

			log.Printf("flushing data.. writing %d executions to db", len(executions))
			e.flushExecutions(batch)

			if err := e.cacheClient.ClearCachedExecutions(context.Background(), keys); err != nil {
				log.Printf("Unable to clear cached executions: %v\n", err)
			}
			batch = nil

		case <-e.ctx.Done():
			e.flushExecutions(batch)
			return
		}
	}
}

func (e *DbEngine) flushExecutions(batch []*models.Execution) {
	if len(batch) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(e.ctx, 120*time.Second)
	defer cancel()

	for _, exec := range batch {
		if err := e.dbHandler.InsertExecution(ctx, exec); err != nil {
			log.Printf("Unable to insert execution: %v\n", err)
			return
		}
	}
}
