INSERT INTO stock_metadata (symbol, name, exchange, mic, currency, type, shares_outstanding, last_updated)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (symbol, exchange) DO UPDATE SET
    name = EXCLUDED.name,
    mic = EXCLUDED.mic,
    currency = EXCLUDED.currency,
    type = EXCLUDED.type,
    shares_outstanding = EXCLUDED.shares_outstanding,
    last_updated = EXCLUDED.last_updated;