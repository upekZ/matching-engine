package db_writer

import (
	"context"
	"database/sql"
	_ "github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/upekZ/matching-engine/internal/models"
	sqlc2 "github.com/upekZ/matching-engine/internal/storage/sqlc"
	"log"
	"os"
	"time"
)

type QueryEngine interface {
}

type CacheClient interface {
	GetExecutions(ctx context.Context) ([]*models.Execution, []string, error)
	ClearCachedExecutions(ctx context.Context, keys []string) error
}

type DbEngine struct {
	executionQueue chan []*models.Execution
	maxBatchSize   int
	batchSize      int
	cacheClient    CacheClient
	ctx            context.Context
	cancel         context.CancelFunc
	queryExec      *sqlc2.Queries
}

func RunDBEngine(ctx context.Context, cacheClient CacheClient, maxBatchSize int, batchSize int) error {
	cancelCtx, cancel := context.WithCancel(ctx)

	connStr := os.Getenv("POSTGRES_CONN")
	if connStr == "" {
		connStr = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		log.Fatalf("Unable to connect to db: %v\n", err)
	}

	engine := &DbEngine{
		executionQueue: make(chan []*models.Execution, batchSize),
		maxBatchSize:   maxBatchSize,
		batchSize:      batchSize,
		cacheClient:    cacheClient,
		ctx:            cancelCtx,
		cancel:         cancel,
		queryExec:      sqlc2.New(db),
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
		if err := e.queryExec.UpsertExecution(ctx, convertToSqlParams(exec)); err != nil {
			log.Printf("failed db execution %s: %v", exec.ExecID, err)
			return
		}
	}
}

func convertToSqlParams(exec *models.Execution) sqlc2.UpsertExecutionParams {
	return sqlc2.UpsertExecutionParams{
		ExecType:     sql.NullString{String: string(exec.ExecType), Valid: true},
		OrdStatus:    sql.NullString{String: string(exec.OrdStatus), Valid: true},
		ClOrdID:      sql.NullString{String: exec.ClOrdID, Valid: true},
		OrderID:      sql.NullString{String: exec.OrderID, Valid: true},
		Symbol:       sql.NullString{String: exec.Symbol, Valid: true},
		Side:         sql.NullString{String: string(exec.Side), Valid: true},
		OrderQty:     sql.NullInt32{Int32: int32(exec.OrderQty), Valid: true},
		Price:        sql.NullFloat64{Float64: exec.Price, Valid: true},
		LastQty:      sql.NullInt32{Int32: int32(exec.LastQty), Valid: true},
		LastPx:       sql.NullFloat64{Float64: exec.LastPx, Valid: true},
		CumQty:       sql.NullInt32{Int32: int32(exec.CumQty), Valid: true},
		LeavesQty:    sql.NullInt32{Int32: int32(exec.LeavesQty), Valid: true},
		ExecID:       exec.ExecID,
		TransactTime: sql.NullInt64{Int64: exec.TransactTime, Valid: true},
		OrdType:      sql.NullString{String: string(exec.OrdType), Valid: true},
	}
}
