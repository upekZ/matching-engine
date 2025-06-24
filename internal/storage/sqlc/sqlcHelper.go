package sqlc

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
	"os"
)

func (q *Queries) convertToSqlParams(exec *models.Execution) UpsertExecutionParams {
	return UpsertExecutionParams{
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

func (q *Queries) InsertExecution(ctx context.Context, exec *models.Execution) error {

	execParams := q.convertToSqlParams(exec)
	_, err := q.db.ExecContext(ctx, upsertExecution,
		execParams.ExecType,
		execParams.OrdStatus,
		execParams.ClOrdID,
		execParams.OrderID,
		execParams.Symbol,
		execParams.Side,
		execParams.OrderQty,
		execParams.Price,
		execParams.LastQty,
		execParams.LastPx,
		execParams.CumQty,
		execParams.LeavesQty,
		execParams.ExecID,
		execParams.TransactTime,
		execParams.OrdType,
	)
	return err
}

func CreateDBHandler() *Queries {
	connAddr := os.Getenv("POSTGRES_ADDR")
	if connAddr == "" {
		log.Printf("environment variable POSTGRES_ADDR not set")
		connAddr = "localhost:5432"
	}

	connStr := fmt.Sprintf("postgres://postgres:postgres@%s/postgres?sslmode=disable", connAddr)

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		log.Fatalf("Unable to connect to db: %v\n", err)
	}

	return New(db)
}
