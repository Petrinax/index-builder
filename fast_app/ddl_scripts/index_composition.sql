CREATE TABLE IF NOT EXISTS index_composition (
    date DATE,
    symbol VARCHAR,
    exchange VARCHAR,
    market_cap REAL,
    price REAL,
    shares REAL,
    weight REAL,
    notional_value REAL,
    top_n INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, symbol, exchange, top_n)
)