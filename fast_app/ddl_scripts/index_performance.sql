CREATE TABLE IF NOT EXISTS index_performance (
    date DATE,
    nav REAL,
    daily_return REAL,
    cumulative_return REAL,
    top_n INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, top_n)
)