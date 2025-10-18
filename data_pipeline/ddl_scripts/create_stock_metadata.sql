
CREATE TABLE IF NOT EXISTS stock_metadata (
    symbol VARCHAR,
    exchange VARCHAR,
    mic VARCHAR,
    name VARCHAR,
    currency VARCHAR,
    type VARCHAR,
    last_updated TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (symbol, exchange)
);

-- Index to optimize queries filtering
CREATE INDEX IF NOT EXISTS idx_stock_metadata_exchange ON stock_metadata(exchange);