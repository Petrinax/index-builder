
CREATE TABLE IF NOT EXISTS daily_stock_prices (
    symbol VARCHAR,
    exchange VARCHAR,
    mic VARCHAR,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,
    date DATE,
    last_updated TIMESTAMP,
    PRIMARY KEY (symbol, exchange, date)
);

-- Index to optimize queries filtering
CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_date ON daily_stock_prices(symbol, date);
CREATE INDEX IF NOT EXISTS idx_stock_prices_exchange ON daily_stock_prices(exchange);
CREATE INDEX IF NOT EXISTS idx_stock_prices_date ON daily_stock_prices(date);
CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_exchange_date ON daily_stock_prices(symbol, exchange, date);