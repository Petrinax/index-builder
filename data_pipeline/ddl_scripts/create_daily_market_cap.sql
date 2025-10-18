
CREATE TABLE IF NOT EXISTS daily_market_cap (
    symbol VARCHAR,
    exchange VARCHAR,
    market_cap BIGINT,
    date DATE,
    last_updated TIMESTAMP,
    PRIMARY KEY (symbol, exchange, date)
);

-- Index to optimize queries filtering
CREATE INDEX IF NOT EXISTS idx_market_cap_symbol_date ON daily_market_cap(symbol, date);
CREATE INDEX IF NOT EXISTS idx_market_cap_exchange ON daily_market_cap(exchange);
CREATE INDEX IF NOT EXISTS idx_market_cap_date ON daily_market_cap(date);
CREATE INDEX IF NOT EXISTS idx_market_cap_symbol_exchange_date ON daily_market_cap(symbol, exchange, date);