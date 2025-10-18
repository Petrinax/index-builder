
CREATE TABLE IF NOT EXISTS ingestion_log (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR NOT NULL,
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_count BIGINT,
    status VARCHAR,
    error_message TEXT
);