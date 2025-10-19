
Sources:

### Alpha Vantage

Pros:
- Free API key with Rate Limit (500 requests/day or 5 requests/min)
- Can use the free API key forever
- Easy to get started and use
- Offers real-time and historical data
- Huge selection of technical indicators**

Cons:
- Rate Limit (500 requests/day or 5 requests/min)
- Limited data availability
- Limited number of stock exchanges covered
- Pricing is not ideal for large scale applications making a lot of simultaneous requests

### iex cloud

Pros:
- offers news, earnings data, and more
- dataset available is huge, commodities/ inflation / interest rates.
Cons:
- Paid tier only
- Works on a credit system where specific endpoints cost more credits than others
- Tricky to calculate the pricing


### Finnhub

Pros:
- Daily/Intraday prices data available
- Extensive free plan
- Well documented and simple to use

Cons:
- Free plan only for US stocks. Not international
- Batch calls in Free tier not available

### yfinance

Pros:
- Completely free. No restrictions
- international stocks data available
- Batch processing in Free tier

Cons:
- Market Cap Data not reliable (Alt. calculate at runtime (outstanding*price))



---


## DuckDB vs SQLite for time series data storage and querying

DuckDB:

Cons:
- Strict File Locks preventing multiple connections to the same DB file

Pros:
- Optimized for analytical queries on large datasets
- Better performance for complex queries


---

Fasten up the INgestion

Short list of ways to speed this up:
Parallelize network I/O (quotes + profiles) with a thread pool (Finnhub calls are I/O-bound).
Respect API rate limits with a concurrency semaphore and small per-call delay rather than serial sleeps.
Batch DB writes inside a single transaction (commit once per batch) to avoid per-row transaction overhead.
Reduce unnecessary profile calls (use cached metadata or skip profile if no market cap needed).
Use DuckDB or bulk CSV/pandas import for large inserts (DuckDB is very fast for analytical loads).
Consider a provider with server-side screener/sort or multiple API keys if allowed.