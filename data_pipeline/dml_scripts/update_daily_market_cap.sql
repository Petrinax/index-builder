-- Update daily_market_cap by joining price data with shares outstanding
INSERT INTO daily_market_cap (symbol, date, market_cap, shares_outstanding, exchange, mic)
SELECT
    p.symbol,
    p.date,
    p.close * m.shares_outstanding AS market_cap,
    m.shares_outstanding,
    p.exchange,
    p.mic
FROM daily_stock_prices p
INNER JOIN stock_metadata m
    ON p.symbol = m.symbol AND p.exchange = m.exchange
WHERE m.shares_outstanding IS NOT NULL
    AND p.close IS NOT NULL
    AND p.close > 0
ON CONFLICT (symbol, date, exchange) DO UPDATE SET
    market_cap = EXCLUDED.market_cap,
    shares_outstanding = EXCLUDED.shares_outstanding,
    mic = EXCLUDED.mic;
