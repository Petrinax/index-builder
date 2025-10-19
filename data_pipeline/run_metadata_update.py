#!/usr/bin/env python3
"""
Standalone script to run stock metadata update (including shares outstanding).
This should be run less frequently than the daily snapshot (e.g., weekly or monthly).

Usage:
    python run_metadata_update.py              # Update all exchanges
    python run_metadata_update.py NYSE         # Update specific exchange
    python run_metadata_update.py NASDAQ
"""

import sys
from ingestion_pipeline import StockDataIngestion
from base_logging import Logger

logger = Logger(__name__)


def main():
    exchange = sys.argv[1] if len(sys.argv) > 1 else None

    try:
        logger.info("="*80)
        logger.info("Running Stock Metadata Update")
        if exchange:
            logger.info(f"Target Exchange: {exchange}")
        else:
            logger.info("Target Exchanges: NYSE, NASDAQ")
        logger.info("="*80)

        ingestion = StockDataIngestion()
        ingestion.run_stock_metadata_update(exchange)
        ingestion.close()

        logger.info("Metadata update completed successfully!")

    except Exception as e:
        logger.error(f"Fatal error during metadata update: {e}")
        raise


if __name__ == "__main__":
    main()

