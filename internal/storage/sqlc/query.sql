-- name: UpsertExecution :exec
INSERT INTO executions (
    exec_type, ord_status, cl_ord_id, order_id, symbol, side, order_qty,
    price, last_qty, last_px, cum_qty, leaves_qty, exec_id, transact_time, ord_type
) VALUES (
             $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
         ) ON CONFLICT (exec_id) DO UPDATE SET
    exec_type = EXCLUDED.exec_type,
    ord_status = EXCLUDED.ord_status,
    cl_ord_id = EXCLUDED.cl_ord_id,
    order_id = EXCLUDED.order_id,
    symbol = EXCLUDED.symbol,
    side = EXCLUDED.side,
    order_qty = EXCLUDED.order_qty,
    price = EXCLUDED.price,
    last_qty = EXCLUDED.last_qty,
    last_px = EXCLUDED.last_px,
    cum_qty = EXCLUDED.cum_qty,
    leaves_qty = EXCLUDED.leaves_qty,
    transact_time = EXCLUDED.transact_time,
    ord_type = EXCLUDED.ord_type;