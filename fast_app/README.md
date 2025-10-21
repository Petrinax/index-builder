# Index Builder FastAPI Application

RESTful API for constructing and querying equal-weighted stock indices based on market capitalization.

## Features

- **Dynamic Index Construction**: Build equal-weighted indices for any date range
- **Historical Performance**: Retrieve daily and cumulative returns
- **Composition Tracking**: View stock compositions for specific dates
- **Change Detection**: Identify when stocks enter or exit the index
- **Excel Export**: Export all data to well-formatted Excel files
- **Caching**: Built-in caching for improved performance

## API Endpoints

### POST /build-index
Construct the equal-weighted index dynamically for given dates.

**Request Body:**
```json
{
  "start_date": "2025-01-01",
  "end_date": "2025-01-31",
  "top_n": 100,
  "initial_nav": 1000.0
}
```

**Response:**
```json
{
  "message": "Index built successfully",
  "start_date": "2025-01-01",
  "end_date": "2025-01-31",
  "top_n": 100,
  "initial_nav": 1000.0,
  "final_nav": 1050.5,
  "total_return": 5.05,
  "days_processed": 21
}
```

### GET /index-performance
Return daily and cumulative returns for the index.

**Query Parameters:**
- `start_date` (required): Start date (YYYY-MM-DD)
- `end_date` (optional): End date (YYYY-MM-DD)

**Example:**
```
GET /index-performance?start_date=2025-01-01&end_date=2025-01-31
```

### GET /index-composition
Return the stock composition for a given date.

**Query Parameters:**
- `date` (required): Date (YYYY-MM-DD)

**Example:**
```
GET /index-composition?date=2025-01-15
```

### GET /composition-changes
List days when composition changed, with stocks entered/exited.

**Query Parameters:**
- `start_date` (required): Start date (YYYY-MM-DD)
- `end_date` (optional): End date (YYYY-MM-DD)

**Example:**
```
GET /composition-changes?start_date=2025-01-01&end_date=2025-01-31
```

### GET /export-excel
Export all index data to an Excel file.

**Query Parameters:**
- `start_date` (required): Start date (YYYY-MM-DD)
- `end_date` (optional): End date (YYYY-MM-DD)

**Example:**
```
GET /export-excel?start_date=2025-01-01&end_date=2025-01-31
```

### DELETE /clear-cache
Clear all cached data.

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up environment variables (optional):
```bash
# Create .env file
DB_TYPE=sqlite
DB_PATH=data/stock_data.db
EXPORT_DIR=fastapi/exports
```

## Running the Application

### Development Mode
```bash
cd /Users/piyushupreti/Documents/Projects/index-builder
python -m uvicorn fastapi.main:app --reload --host 0.0.0.0 --port 8000
```

### Production Mode
```bash
python -m uvicorn fastapi.main:app --host 0.0.0.0 --port 8000 --workers 4
```

## API Documentation

Once the server is running, access:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## Index Construction Algorithm

Based on `build_steps.txt`, the algorithm:

1. **INIT Phase** (day before start_date):
   - Set initial NAV = 1000 (or custom value)
   - Fetch top N stocks by market cap
   - Distribute shares equally weighted by value

2. **ITERATE Phase** (start_date to end_date):
   - Fetch previous day's portfolio
   - Calculate NAV at end of day with updated prices
   - Calculate daily returns: (NAV1 - NAV0) / NAV0 * 100
   - Calculate cumulative returns: (NAV1 - base) / base * 100
   - Fetch top N stocks for reconstitution
   - Detect composition changes (add/remove stocks)
   - Rebalance portfolio with equal weighting
   - Persist portfolio and performance data

## Database Schema

### index_performance
- date, nav, daily_return, cumulative_return, top_n

### index_composition
- date, symbol, exchange, market_cap, price, shares, weight, notional_value, top_n

### composition_changes
- date, symbol, exchange, change_type, market_cap, top_n

## Architecture

```
fastapi/
├── main.py          # FastAPI application and routes
├── models.py        # Pydantic request/response models
├── services.py      # Core business logic
├── config.py        # Configuration settings
└── exports/         # Generated Excel files
```

## Example Usage

### 1. Build Index for January 2025
```bash
curl -X POST "http://localhost:8000/build-index" \
  -H "Content-Type: application/json" \
  -d '{
    "start_date": "2025-01-01",
    "end_date": "2025-01-31",
    "top_n": 100,
    "initial_nav": 1000.0
  }'
```

### 2. Get Performance Data
```bash
curl "http://localhost:8000/index-performance?start_date=2025-01-01&end_date=2025-01-31"
```

### 3. Get Composition for Specific Date
```bash
curl "http://localhost:8000/index-composition?date=2025-01-15"
```

### 4. Export to Excel
```bash
curl "http://localhost:8000/export-excel?start_date=2025-01-01&end_date=2025-01-31" \
  --output index_export.xlsx
```

## Notes

- All responses are in JSON format
- Performance data is cached for improved response times
- Excel exports include multiple sheets: Performance, Summary, and Composition Changes
- The API automatically handles non-trading days
- Market cap data must be available in the database before building the index

