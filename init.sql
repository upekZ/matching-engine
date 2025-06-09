CREATE TABLE IF NOT EXISTS executions (
    exec_id VARCHAR PRIMARY KEY,
    exec_type VARCHAR,
    ord_status VARCHAR,
    cl_ord_id VARCHAR,
    order_id VARCHAR,
    symbol VARCHAR,
    side VARCHAR,
    order_qty INTEGER,
    price DOUBLE PRECISION,
    last_qty INTEGER,
    last_px DOUBLE PRECISION,
    cum_qty INTEGER,
    leaves_qty INTEGER,
    transact_time BIGINT,
    ord_type VARCHAR
);