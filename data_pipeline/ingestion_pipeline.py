import os
import time
from datetime import datetime
from typing import List, Dict
from dotenv import load_dotenv
from database import DatabaseFactory, DatabaseConnection
from client import ClientFactory, StockDataClient

from base_logging import Logger

load_dotenv()


logger = Logger(__name__)

# Configuration
DATA_PROVIDER = os.getenv('DATA_PROVIDER', 'yfinance')  # Default provider
DB_TYPE = os.getenv('DB_TYPE', 'sqlite')  # Default to sqlite
DB_PATH = f'stock_data.{DB_TYPE}' if DB_TYPE == 'duckdb' else 'stock_data.db'

EXCHANGE_MIC_CODES = {
    'NYSE': ['XNYS'],
    'NASDAQ': ['XNAS']
}


class StockDataIngestion:
    def __init__(self, db_type: str = 'sqlite', db_path: str = 'stock_data.db', data_provider: str = None):
        # Initialize data provider client
        self.data_provider = data_provider or DATA_PROVIDER
        self.client: StockDataClient = ClientFactory.create(self.data_provider)

        logger.info(f"Using {self.data_provider} as data provider")

        self.db_type = db_type or DB_TYPE
        self.db_path = db_path or DB_PATH
        self.db_conn: DatabaseConnection = DatabaseFactory.create(self.db_type, self.db_path)
        logger.info(f"Using {self.db_type.upper()} database: {self.db_path}")


    # DB Tasks

    def _update_stock_metadata(self, stocks: List[Dict]):
        for stock in stocks:
            try:
                self.db_conn.execute("""
                    INSERT OR IGNORE INTO stock_metadata (symbol, name, exchange, mic)
                    VALUES (?, ?, ?, ?)
                """, [stock['symbol'], stock['name'], stock['exchange'], stock['mic']])
            except Exception as e:
                logger.error(f"Error updating metadata for {stock['symbol']}: {e}")
        self.db_conn.execute("COMMIT")
        logger.info("Stock metadata updated successfully")

    def _store_price(self, symbol: str, quote: Dict, date: str, exchange: str, mic: str):
        """
        This statement performs an *upsert* into the `daily_stock_prices` table.

        - Insert a new daily price row, or update the existing row if one already exists for the same `(symbol, date, exchange)` key.
        - Conflict target: `(symbol, date, exchange)` matches the table`s unique constraint so duplicates trigger the conflict branch.
        - Update behavior: the `DO UPDATE SET` uses `EXCLUDED.<col>` (the incoming values) to overwrite `open, high, low, close, volume, mic` on the existing row.
        - Result: ensures a single row per `(symbol, date, exchange)` with the latest price and mic, avoiding duplicate inserts.
        - Compatibility / notes: this syntax is supported by SQLite (3.24+) and PostgreSQL; ensure the unique constraint or primary key exists for the conflict columns. The `?` placeholders follow the DB-API parameter binding.

        """
        self.db_conn.execute('''
            INSERT INTO daily_stock_prices (symbol, date, open, high, low, close, volume, exchange, mic)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol, date, exchange) DO UPDATE SET
                open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                close = EXCLUDED.close, volume = EXCLUDED.volume, mic = EXCLUDED.mic
        ''', [symbol, date, quote.get('open'), quote.get('high'), quote.get('low'),
              quote.get('close'), quote.get('volume'), exchange, mic])

    def _store_market_cap(self, symbol: str, market_cap: float, shares: float,
                          date: str, exchange: str, mic: str):
        """
        This statement performs an *upsert* into the `daily_market_cap` table.
        - Insert a new daily market cap row, or update the existing row if one already exists for the same `(symbol, date, exchange)` key.
        - Conflict target: `(symbol, date, exchange)` matches the table`s unique constraint so duplicates trigger the conflict branch.
        - Update behavior: the `DO UPDATE SET` uses `EXCLUDED.<col>` (the incoming values) to overwrite `market_cap, shares_outstanding, mic` on the existing row.
        - Result: ensures a single row per `(symbol, date, exchange)` with the latest market cap data, avoiding duplicate inserts.

        - Compatibility / notes: this syntax is supported by SQLite (3.24+) and PostgreSQL; ensure the unique constraint or primary key exists for the conflict columns. The `?` placeholders follow the DB-API parameter binding.

        """
        self.db_conn.execute('''
            INSERT INTO daily_market_cap (symbol, date, market_cap, shares_outstanding, exchange, mic)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol, date, exchange) DO UPDATE SET
                market_cap = EXCLUDED.market_cap, shares_outstanding = EXCLUDED.shares_outstanding, mic = EXCLUDED.mic
        ''', [symbol, date, market_cap, shares, exchange, mic])

    def close(self):
        if self.db_conn:
            self.db_conn.close()
            logger.info("Database connection closed")


    # Ingestion Logs

    def _create_ingestion_log(self, exchange: str, target_date: str, started_at: datetime) -> int:
        """Create a new ingestion log entry and return its ID."""
        self.db_conn.execute(
            "INSERT INTO ingestion_log (exchange, date, started_at, status) VALUES (?, ?, ?, 'RUNNING')",
            [exchange, target_date, started_at]
        )
        return self.db_conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    def _update_ingestion_log_success(self, log_id: int, processed: int,
                                      successful: int, failed: int, exchange: str):
        """Update ingestion log with success status."""
        self.db_conn.execute('''
            UPDATE ingestion_log SET stocks_processed = ?, stocks_successful = ?, 
            stocks_failed = ?, completed_at = ?, status = 'COMPLETED' WHERE id = ?
        ''', [processed, successful, failed, datetime.now(), log_id])

        logger.info(f"{exchange} completed: {successful}/{processed} successful")

    def _update_ingestion_log_failure(self, log_id: int, error_message: str):
        """Update ingestion log with failure status."""
        self.db_conn.execute(
            "UPDATE ingestion_log SET status = 'FAILED', completed_at = ?, error_message = ? WHERE id = ?",
            [datetime.now(), str(error_message), log_id]
        )

    # Stock Data Fetching

    def _get_stocks(self, exchange: str) -> List[Dict]:
        try:
            stocks = self.client.get_stock_symbols(exchange)
            logger.info(f"Found {len(stocks)} stocks for {exchange}")
            return stocks
        except Exception as e:
            logger.error(f"Error fetching stocks: {e}")
            return []

    def _process_stocks_batch(self, stocks: List[Dict], target_date: str, exchange: str) -> tuple:
        """
        Process a batch of stocks and return processing statistics.
        Returns: (processed_count, successful_count, failed_count)
        """
        processed = successful = failed = 0
        total_stocks = len(stocks)

        for stock in stocks:
            processed += 1

            if self._process_single_stock(stock, target_date, exchange):
                successful += 1
            else:
                failed += 1

            # Log progress at 5% intervals
            if processed * 100 / total_stocks % 5 == 0:
                logger.info(f"Processed {processed}/{total_stocks} stocks for {exchange}")

        return processed, successful, failed

    def _process_single_stock(self, stock: Dict, target_date: str, exchange: str) -> bool:
        """
        Process a single stock: fetch quote and market cap, store data.
        Returns True if successful, False otherwise.
        """
        symbol = stock.get('symbol')
        mic = stock.get('mic', '')

        try:
            quote = self.client.get_quote(symbol)
            if not quote or quote.get('close') == 0:
                return False

            self._store_price(symbol, quote, target_date, exchange, mic)

            profile = self.client.get_company_profile(symbol)
            if profile:
                market_cap = profile.get('market_cap')
                shares = profile.get('shares_outstanding')

                if not market_cap and shares:
                    market_cap = quote.get('close') * shares

                if market_cap:
                    self._store_market_cap(symbol, market_cap, shares, target_date, exchange, mic)

            return True

        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            return False

    # Main Ingestion Logic

    def ingest_daily_snapshot_data(self, exchange: str, target_date: str = None):
        target_date = target_date or datetime.now().strftime('%Y-%m-%d')
        started_at = datetime.now()
        log_id = None

        try:
            logger.info(f"Starting ingestion for {exchange} on {target_date}")

            # Create ingestion log entry
            log_id = self._create_ingestion_log(exchange, target_date, started_at)

            # Get and filter stocks
            exchange_stocks = self._get_stocks(exchange)
            if not exchange_stocks:
                return

            # Update stock metadata
            self._update_stock_metadata(exchange_stocks)

            # Process all stocks
            processed, successful, failed = self._process_stocks_batch(
                exchange_stocks, target_date, exchange
            )

            # Update log with success
            self._update_ingestion_log_success(log_id, processed, successful, failed, exchange)

        except Exception as e:
            logger.error(f"Ingestion error for {exchange}: {e}")
            if log_id:
                self._update_ingestion_log_failure(log_id, e)
            raise

    def run_daily_snapshot(self, target_date: str = None):
        logger.info("="*80)
        logger.info(f"Starting Daily Stock Data Snapshot - NYSE & NASDAQ (Provider: {self.data_provider})")
        logger.info("="*80)

        logger.info("=" * 80)
        logger.info("Daily Snapshot Completed")
        logger.info("=" * 80)
        self._print_summary(target_date)

    def _print_summary(self, date: str = None):
        date = date or datetime.now().strftime('%Y-%m-%d')
        try:
            result = self.db_conn.execute('''
                SELECT exchange, COUNT(DISTINCT symbol) as stock_count
                FROM daily_stock_prices WHERE date = ? GROUP BY exchange ORDER BY exchange
            ''', [date]).fetchall()

            logger.info(f"\n{'='*50}\nData Summary for {date}\n{'='*50}")
            for row in result:
                logger.info(f"{row[0]}: {row[1]} stocks")
            logger.info("="*50)
        except Exception as e:
            logger.warning(f"Could not generate summary: {e}")



def main():
    try:
        # You can now easily switch providers:
        # ingestion = StockDataIngestion(db_type='duckdb', data_provider='finnhub')
        # ingestion = StockDataIngestion(db_type='duckdb', data_provider='alphavantage')
        # ingestion = StockDataIngestion(db_type='sqlite', data_provider='iexcloud')

        ingestion = StockDataIngestion('sqlite')  # Uses defaults from env vars
        ingestion.run_daily_snapshot()
        ingestion.close()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()


