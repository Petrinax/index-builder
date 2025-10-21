import os
from datetime import datetime
from typing import List, Dict, Optional
from dotenv import load_dotenv
import pandas as pd
from .database import DatabaseFactory, DatabaseConnection
from .client import ClientFactory, StockDataClient
from concurrent.futures import ThreadPoolExecutor, as_completed

from .base_logging import Logger

load_dotenv()


logger = Logger("pipeline.ingestoin")

# Configuration
DATA_PROVIDER = os.getenv('DATA_PROVIDER', 'yfinance')  # Default provider
DB_TYPE = os.getenv('DB_TYPE', 'sqlite')  # Default to sqlite
DB_PATH = os.getenv('DB_PATH', 'stock_data.db')
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '15'))  # Number of parallel workers for metadata updates

EXCHANGE_MIC_CODES = {
    'NYSE': ['XNYS'],
    'NASDAQ': ['XNAS']
}


class StockDataIngestion:
    def __init__(self, db_type: str = 'sqlite', db_path: str = None, data_provider: str = None):
        # Initialize data provider client
        self.data_provider = data_provider or DATA_PROVIDER
        self.client: StockDataClient = ClientFactory.create(self.data_provider)

        logger.info(f"Using {self.data_provider} as data provider")

        self.db_type = db_type or DB_TYPE
        self.db_path = db_path or DB_PATH
        self.db_client: DatabaseConnection = DatabaseFactory.create(self.db_type, self.db_path)
        self.run_start_ts = self.run_end_ts = None
        logger.info(f"Using {self.db_type.upper()} database: {self.db_path}")


    # DB Tasks

    def close(self):
        if self.db_client:
            self.db_client.close()
            logger.info("Database connection closed")

    # Meta Data Update

    def _fetch_and_update_single_stock_metadata(self, stock: Dict) -> Dict[str, any]:
        """
        Fetch company profile and prepare metadata for a single stock.
        This function is designed to be called in parallel.

        Returns:
            Dict with 'success', 'symbol', 'data', and 'error' keys
        """
        symbol = stock['symbol']
        try:
            # Fetch company profile to get shares_outstanding
            profile = self.client.get_company_profile(symbol)
            shares = profile.get('shares_outstanding') if profile else None

            return {
                'success': True,
                'symbol': symbol,
                'data': {
                    'symbol': symbol,
                    'name': stock['name'],
                    'exchange': stock['exchange'],
                    'mic': stock['mic'],
                    'currency': stock.get('currency', 'USD'),
                    'type': stock.get('type', 'Common Stock'),
                    'shares': shares,
                    'timestamp': datetime.now()
                },
                'error': None
            }
        except Exception as e:
            return {
                'success': False,
                'symbol': symbol,
                'data': None,
                'error': str(e)
            }

    def _update_stock_metadata(self, stocks: List[Dict]):
        """
        Update stock metadata including shares outstanding with bulk operations.
        Includes detailed progress logging.
        """
        total_stocks = len(stocks)
        logger.info(f"Starting metadata update for {total_stocks} stocks")

        successful = 0
        failed = 0
        processed = 0

        # List to accumulate all successful data for bulk insert
        bulk_data = []

        # Track timing
        start_time = datetime.now()
        last_log_time = start_time

        # Fetch all profiles first
        logger.info("Fetching company profiles...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_stock = {executor.submit(self._fetch_and_update_single_stock_metadata, stock): stock for stock in stocks}
            for future in as_completed(future_to_stock):
                processed += 1
                result = future.result()

                if result['success']:
                    successful += 1
                    bulk_data.append(result['data'])
                else:
                    failed += 1
                    logger.error(f"Error fetching metadata for {result['symbol']}: {result['error']}")

                # Log progress every 100 stocks or every 5 seconds
                current_time = datetime.now()
                if processed % 100 == 0 or (current_time - last_log_time).seconds >= 5:
                    elapsed = (current_time - start_time).total_seconds()
                    rate = processed / elapsed if elapsed > 0 else 0
                    eta_seconds = (total_stocks - processed) / rate if rate > 0 else 0

                    logger.info(
                        f"Fetching Progress: {processed}/{total_stocks} ({processed*100/total_stocks:.1f}%) | "
                        f"Success: {successful} | Failed: {failed} | "
                        f"Rate: {rate:.1f} stocks/sec | ETA: {eta_seconds/60:.1f} min"
                    )
                    last_log_time = current_time

        # Bulk insert/update all successful stocks
        if bulk_data:
            logger.info(f"Bulk updating {len(bulk_data)} stocks in database...")
            db_start = datetime.now()
            try:

                # Build a single multi-row upsert to avoid executing per-record
                placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?)"] * len(bulk_data))
                params = []
                for data in bulk_data:
                    params.extend([
                        data['symbol'],
                        data['name'],
                        data['exchange'],
                        data['mic'],
                        data['currency'],
                        data['type'],
                        data['shares'],
                        data['timestamp']
                    ])

                sql = f"""
                    INSERT INTO stock_metadata (symbol, name, exchange, mic, currency, type, shares_outstanding, last_updated)
                    VALUES {placeholders}
                    ON CONFLICT (symbol, exchange) DO UPDATE SET
                        name = EXCLUDED.name,
                        mic = EXCLUDED.mic,
                        currency = EXCLUDED.currency,
                        type = EXCLUDED.type,
                        shares_outstanding = EXCLUDED.shares_outstanding,
                        last_updated = EXCLUDED.last_updated
                """
                self.db_client.execute(sql, params)

                # # Commit all changes at once
                # self.db_client.execute("COMMIT")
                db_elapsed = (datetime.now() - db_start).total_seconds()
                logger.info(f"Successfully updated {len(bulk_data)} stocks in database in {db_elapsed:.2f} seconds")
            except Exception as e:
                logger.error(f"Error during bulk update: {e}")
                raise

        # Final summary
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"Metadata update completed in {elapsed/60:.2f} minutes")
        logger.info(f"Total: {total_stocks} | Successful: {successful} | Failed: {failed}")
        logger.info(f"Average rate: {total_stocks/elapsed:.1f} stocks/sec")

    def _get_stocks_metadata(self, exchange: str) -> pd.DataFrame:
        """Fetch stock metadata for a specific exchange"""
        try:
            df_meta = pd.read_sql(
                sql="""
                    SELECT symbol, name, exchange, mic, currency, type, shares_outstanding 
                    FROM stock_metadata 
                    WHERE exchange = ? AND shares_outstanding IS NOT NULL
                """, params= [exchange], con=self.db_client.conn
            )
            return df_meta
        except Exception as e:
            logger.error(f"Error fetching stock metadata for {exchange}: {e}")
            raise e


    # Stock Data Update

    def _get_stocks(self, exchange: str) -> List[Dict]:
        try:
            stocks = self.client.get_stock_symbols(exchange)
            logger.info(f"Found {len(stocks)} stocks for {exchange}")
            return stocks
        except Exception as e:
            logger.error(f"Error fetching stocks: {e}")
            return []

    def _process_stocks_batch(self, stocks: pd.DataFrame, target_date: Optional[str], period: Optional[str] ,exchange: str) -> tuple:
        """
        Process a batch of stocks and return processing statistics.
        Returns: (processed_count, successful_count, failed_count)
        """
        processed = successful = failed = 0
        total_stocks = len(stocks)
        requested_symbols = stocks['symbol'].to_list()

        quotes = self.client.get_batch_quote(requested_symbols, target_date=target_date, period=period,
                                             load_ts=self.run_start_ts)

        # Use upsert approach instead of append to handle conflicts
        if not quotes.empty:
            # Write to temporary table first
            temp_table = "temp_daily_stock_prices"
            quotes.to_sql(temp_table, self.db_client.conn, if_exists="replace", index=False)

            # Perform upsert from temp table to main table
            upsert_sql = f"""
                INSERT OR REPLACE INTO daily_stock_prices (symbol, exchange, mic, open, high, low, close, volume, date, last_updated)
                SELECT symbol, exchange, mic, open, high, low, close, volume, date, last_updated
                FROM {temp_table}
            """
            self.db_client.execute(upsert_sql)
            
            # Drop temporary table
            self.db_client.execute(f"DROP TABLE IF EXISTS {temp_table}")
            
            logger.info(f"Successfully upserted {len(quotes)} price records")

        fetched_symbols = set(quotes["symbol"].unique())

        # Compute counts
        success_count = len(fetched_symbols)
        failed_symbols = set(requested_symbols) - fetched_symbols
        failed_count = len(failed_symbols)

        logger.info(f"✅ Successfully fetched: {success_count}")
        logger.info(f"❌ Failed to fetch: {failed_count}")
        if failed_symbols:
            logger.info(f"Failed symbols: {sorted(failed_symbols)}")

        return processed, successful, failed, quotes

    # Main Ingestion Logic

    def ingest_daily_snapshot_data(self, exchange: str, target_date: str = None, period: str = None):

        if target_date and period:
            raise ValueError("Specify either target_date or period, not both.")
        if not target_date:
            period = period or '1d'

        started_at = datetime.now()
        # log_id = None

        try:
            logger.info(f"Starting ingestion for {exchange} on {target_date}")

            # Create ingestion log entry
            # log_id = self._create_ingestion_log(exchange, target_date, started_at)

            # Get and filter stocks
            df_stocks_meta = self._get_stocks_metadata(exchange)
            if df_stocks_meta.empty:
                return

            # Process all stocks (using stored shares_outstanding from metadata)
            processed, successful, failed, df_quotes = self._process_stocks_batch(
                df_stocks_meta, target_date, period, exchange
            )

            # Calculate Market Cap for all successfully processed stocks
            if not df_quotes.empty:
                df_joined = pd.merge(
                    df_quotes[["symbol", "exchange", "mic", "close", "date"]],
                    df_stocks_meta[["symbol", "exchange", "shares_outstanding"]],
                    on=["symbol", "exchange"],
                    how="inner"
                )
                
                # Calculate market cap: shares_outstanding * close price
                df_joined['market_cap'] = df_joined['shares_outstanding'] * df_joined['close']
                df_joined['last_updated'] = datetime.now()
                
                df_market_cap = df_joined[[
                    "symbol", "exchange", "mic", "market_cap", "shares_outstanding", "date", "last_updated"
                ]]
                
                # Use upsert approach for market cap as well
                temp_table = "temp_daily_market_cap"
                df_market_cap.to_sql(temp_table, self.db_client.conn, if_exists="replace", index=False)
                
                upsert_sql = f"""
                    INSERT OR REPLACE INTO daily_market_cap (symbol, exchange, mic, market_cap, shares_outstanding, date, last_updated)
                    SELECT symbol, exchange, mic, market_cap, shares_outstanding, date, last_updated
                    FROM {temp_table}
                """
                self.db_client.execute(upsert_sql)
                self.db_client.execute(f"DROP TABLE IF EXISTS {temp_table}")
                
                logger.info(f"Successfully upserted {len(df_market_cap)} market cap records")

            # Update log with success
            # self._update_ingestion_log_success(log_id, processed, successful, failed, exchange)

        except Exception as e:
            logger.error(f"Ingestion error for {exchange}: {e}")
            # if log_id:
            #     self._update_ingestion_log_failure(log_id, str(e))
            raise

    def run_stock_metadata_update(self, exchange: str = 'NYSE'):
        """
        Update stock metadata including shares outstanding.
        This should run less frequently (e.g., weekly or monthly) as shares outstanding
        doesn't change frequently.

        Args:
            exchange: Specific exchange to update, or None to update all (NYSE, NASDAQ)
        """
        logger.info("="*80)
        logger.info(f"Starting Stock Metadata Update (Provider: {self.data_provider})")
        logger.info("="*80)


        try:
            logger.info(f"Updating metadata for {exchange}")

            # Get stocks for exchange
            stocks = self._get_stocks(exchange)
            if not stocks:
                logger.warning(f"No stocks found for {exchange}")
                raise ValueError(f"No stocks found for {exchange}")

            # Update metadata with shares outstanding
            self._update_stock_metadata(stocks)

            logger.info(f"Metadata update completed for {exchange}")

        except Exception as e:
            logger.error(f"Error updating metadata for {exchange}: {e}")

        logger.info("="*80)
        logger.info("Stock Metadata Update Completed")
        logger.info("="*80)

    def run_daily_snapshot(self, exchange: str = 'NYSE' ,target_date: str = None, period: str = None):

        logger.info("="*80)
        logger.info(f"Starting Daily Stock Data Snapshot - NYSE & NASDAQ (Provider: {self.data_provider})")
        logger.info("="*80)
        self.run_start_ts = datetime.now()

        # Run ingestion for both exchanges
        try:
            self.ingest_daily_snapshot_data(exchange, target_date, period)
        except Exception as e:
            logger.error(f"Failed to process {exchange}: {e}")

        logger.info("=" * 80)
        logger.info("Daily Snapshot Completed")
        logger.info("=" * 80)
        self.run_end_ts = datetime.now()
        logger.info(f"Run Duration: {(self.run_end_ts - self.run_start_ts).total_seconds()} seconds")
        self._print_summary(self.run_start_ts.strftime("%Y-%m-%d %H:%M:%S.%f"))

    def _print_summary(self, load_ts: str = None):
        try:
            result = self.db_client.execute('''
                SELECT exchange, COUNT(DISTINCT symbol) as stock_count
                FROM daily_stock_prices WHERE last_updated = ? GROUP BY exchange ORDER BY exchange
            ''', [load_ts]).fetchall()

            logger.info(f"\n{'='*50}\nData Summary for load time: {load_ts}\n{'='*50}")
            for row in result:
                logger.info(f"{row[0]}: {row[1]} stocks")
            logger.info("="*50)
        except Exception as e:
            logger.warning(f"Could not generate summary: {e}")


def main():
    """
    Python script with args to specify run_daily_snapshot or run_stock_metadata_update, with all params.

    Usage:
        python data_pipeline/ingestion_pipeline.py run_daily_snapshot --exchange NYSE --date 2024-10-17
        python data_pipeline/ingestion_pipeline.py run_daily_snapshot --exchange NYSE --period 1d
        python data_pipeline/ingestion_pipeline.py run_stock_metadata_update --exchange NYSE

    """
    import argparse

    parser = argparse.ArgumentParser(description="Stock Data Ingestion Pipeline")
    subparsers = parser.add_subparsers(dest='command', required=True)

    # Subparser for daily snapshot
    snapshot_parser = subparsers.add_parser('run_daily_snapshot', help='Run daily stock data snapshot ingestion')
    snapshot_parser.add_argument('--exchange', type=str, default='NYSE', help='Exchange to process (default: NYSE)')
    snapshot_parser.add_argument('--date', type=str, help='Target date for snapshot (YYYY-MM-DD)')
    snapshot_parser.add_argument('--period', type=str, help='Period for snapshot (e.g., 1d, 5d), Valid periods: 1d,5d,1mo,3mo,6mo,1y,ytd')

    # Subparser for metadata update
    metadata_parser = subparsers.add_parser('run_stock_metadata_update', help='Update stock metadata including shares outstanding')
    metadata_parser.add_argument('--exchange', type=str, default='NYSE', help='Exchange to update (default: NYSE)')

    args = parser.parse_args()

    ingestion = StockDataIngestion()

    if args.command == 'run_daily_snapshot':
        ingestion.run_daily_snapshot(exchange=args.exchange, target_date=args.date, period=args.period)
    elif args.command == 'run_stock_metadata_update':
        ingestion.run_stock_metadata_update(exchange=args.exchange)

    ingestion.close()

if __name__ == "__main__":
    main()