CREATE TABLE IF NOT EXISTS executions (
    exec_id VARCHAR(255) PRIMARY KEY,
    exec_type VARCHAR(50),
    ord_status VARCHAR(50),
    cl_ord_id VARCHAR(255),
    order_id VARCHAR(255),
    symbol VARCHAR(50),
    side VARCHAR(20),
    order_qty INTEGER,
    price DOUBLE PRECISION,
    last_qty INTEGER,
    last_px DOUBLE PRECISION,
    cum_qty INTEGER,
    leaves_qty INTEGER,
    transact_time BIGINT,
    ord_type VARCHAR(50)
    );