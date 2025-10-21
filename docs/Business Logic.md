# Business Logic Documentation

## Index Builder Strategy & Implementation

This document details the business logic, mathematical models, and algorithms used in the Index Builder system for constructing equal-weighted stock indices.

## Strategy Definition: Equal Notional Weight

### Overview
The Index Builder implements an **Equal Notional Weight** strategy, which allocates the same dollar amount to each stock in the index, regardless of the stock's price or market capitalization.

### Mathematical Foundation

#### Equal Weight Formula
For an index with `N` stocks and total NAV of `V`:
- **Weight per stock**: `w_i = 1/N` (equal percentage weight)
- **Notional value per stock**: `n_i = V × w_i = V/N`
- **Shares per stock**: `s_i = n_i / P_i` where `P_i` is the price of stock `i`

#### NAV Calculation
```
NAV(t) = Σ(s_i × P_i(t)) for i = 1 to N
```
Where:
- `NAV(t)` = Net Asset Value at time t
- `s_i` = Number of shares of stock i (constant until rebalancing)
- `P_i(t)` = Price of stock i at time t

#### Return Calculations
- **Daily Return**: `R_daily = (NAV_t - NAV_{t-1}) / NAV_{t-1} × 100`
- **Cumulative Return**: `R_cumulative = (NAV_t - NAV_0) / NAV_0 × 100`

## Index Build Logic

### Two-Phase Process

#### Phase 1: INIT (Initialization)
```
Input: start_date, end_date, top_n, initial_nav
```

1. **Find Initial Trading Day**
   - Target: `start_date - 1` (day before start)
   - Search backward up to 10 days for valid trading data
   - Ensures portfolio establishment before tracking period

2. **Stock Selection**
   - Query top N stocks by market capitalization
   - Filter: `market_cap IS NOT NULL AND close > 0`
   - Sort: `ORDER BY market_cap DESC LIMIT N`

3. **Initial Portfolio Construction**
   - Calculate equal weights: `weight = 1/N`
   - Calculate notional per stock: `notional = initial_nav / N`
   - Calculate shares: `shares = notional / price`
   - Store initial composition

#### Phase 2: ITERATE (Daily Processing)
```
For each trading day from start_date to end_date:
```

1. **Portfolio Valuation**
   - Update prices for existing positions
   - Calculate new NAV: `NAV = Σ(shares_i × price_i)`
   - Calculate daily return vs previous NAV

2. **Reconstitution Analysis**
   - Fetch current top N stocks by market cap
   - Compare with existing portfolio holdings
   - Identify additions and removals

3. **Rebalancing**
   - If composition changes OR periodic rebalancing
   - Redistribute total NAV equally among new top N stocks
   - Calculate new share allocations

4. **Data Persistence**
   - Store daily performance metrics
   - Store daily composition snapshot
   - Store composition changes (if any)
