package db_writer

import (
	"context"
	"errors"
	_ "github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/upekZ/matching-engine/internal/models"

	"log"
	"time"
)

type ExecutionFeeder interface {
	GetExecutions(ctx context.Context) ([]*models.Execution, []string, error)
	ClearStoredExecutions(ctx context.Context, keys []string) error
}

type DBHandler interface {
	InsertExecution(ctx context.Context, exec *models.Execution) error
}

type DbEngine struct {
	executionQueue  chan []*models.Execution
	executionClient ExecutionFeeder
	ctx             context.Context
	cancel          context.CancelFunc
	dbHandler       DBHandler
}

func New(ctx context.Context, dbHandler DBHandler, cacheClient ExecutionFeeder) error {
	cancelCtx, cancel := context.WithCancel(ctx)

	engine := &DbEngine{
		executionQueue:  make(chan []*models.Execution, 1000),
		executionClient: cacheClient,
		ctx:             cancelCtx,
		cancel:          cancel,
		dbHandler:       dbHandler,
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

		case <-ticker.C:
			executions, keys, err := e.executionClient.GetExecutions(context.Background())
			if err == nil && len(keys) > 0 {
				e.executionQueue <- executions
			} else {
				continue
			}

			log.Printf("flushing data.. writing %d executions to db", len(executions))
			if err := e.flushExecutions(executions); err != nil {
				log.Printf("Unable to flush executions: %v\n", err)
			}

			if err := e.executionClient.ClearStoredExecutions(context.Background(), keys); err != nil {
				log.Printf("Unable to clear cached executions: %v\n", err)
			}
			batch = nil

		case <-e.ctx.Done():
			if err := e.flushExecutions(batch); err != nil {
				log.Printf("Unable to flush executions: %v\n", err)
			}
			return
		}
	}
}

func (e *DbEngine) flushExecutions(batch []*models.Execution) error {
	if len(batch) == 0 {
		return errors.New("empty batch")
	}
	ctx, cancel := context.WithTimeout(e.ctx, 120*time.Second)
	defer cancel()

	for _, exec := range batch {
		if err := e.dbHandler.InsertExecution(ctx, exec); err != nil {
			log.Printf("Unable to insert execution: %v\n", err)
			return errors.New("failed to insert execution")
		}
	}

	return nil
}
