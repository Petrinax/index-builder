CREATE TABLE IF NOT EXISTS composition_changes (
    date DATE,
    symbol VARCHAR,
    exchange VARCHAR,
    change_type VARCHAR,
    market_cap REAL,
    top_n INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, symbol, exchange, change_type, top_n)
)