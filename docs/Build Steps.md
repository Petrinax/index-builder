
# Build Index Logic

input params:
start, ends

## INIT:


    # simulates/marks start of day (start - 1)
    Let the day end ...
    # simulates/marks end of day (start - 1)
    
**Event** : start-1 day ends

    1. set NAV = 1000
    
    2. fetch top N stocks at EOD (start - 1) [handle error if start - 1 not found]

    3. Distribute shares equally weighted by value. Strategy: (Equal Notional Weighting) # scalable to add strategies later

    4. Maintain share distribution/portfolio till EOD of `start`

## ITERATE:

iterate through start - end:

    # simulates/marks start of day `day`
    Let the day end ...
    # simulates/marks end of day `day`

**Event** : day ends

    5. Fetch portfolio from (day - 1)

    6. Calculate NAV at EOD: day

    # Reporting

    7. Calculate daily returns (NAV1 - NAV0)/NAV0 * 100

    8. Calculate cumulative returns (NAV1 - base)/base * 100

    # Re Constitution

    9. Fetch top N stocks at EOD `day`

    10. Add/Delete stocks from portfolio (if top N list changes).

    # Re balance

    11. Re balance portfolio with Strategy: Equal Notional Weighting)

    # Log portfolio
    12. { date: portfolio }

