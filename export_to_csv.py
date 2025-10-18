"""Export all SQLite database tables to CSV files"""

import os
import csv
from datetime import datetime
from data_pipeline.database import DatabaseFactory
from data_pipeline.base_logging import Logger

logger = Logger(__name__)

DB_PATH = 'data_pipeline/stock_data.db'
OUTPUT_DIR = 'csv_exports'

def export_sqlite_to_csv():
    """Export all tables from SQLite database to CSV files"""

    # Create output directory
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        logger.info(f"Created output directory: {OUTPUT_DIR}")

    # Connect to database
    db_conn = DatabaseFactory.create('sqlite', DB_PATH)
    logger.info(f"Connected to database: {DB_PATH}")

    try:
        # Get all table names
        db_conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in db_conn.fetchall()]

        if not tables:
            logger.warning("No tables found in database")
            return

        logger.info(f"Found {len(tables)} tables: {', '.join(tables)}")

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Export each table
        for table_name in tables:
            try:
                # Get column names
                db_conn.execute(f"PRAGMA table_info({table_name})")
                columns = [row[1] for row in db_conn.fetchall()]

                # Get all data
                db_conn.execute(f"SELECT * FROM {table_name}")
                rows = db_conn.fetchall()

                # Create CSV file
                csv_filename = f"{OUTPUT_DIR}/{table_name}_{timestamp}.csv"

                with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)

                    # Write header
                    writer.writerow(columns)

                    # Write data rows
                    writer.writerows(rows)

                logger.info(f"✓ Exported {table_name}: {len(rows)} rows → {csv_filename}")

            except Exception as e:
                logger.error(f"✗ Error exporting {table_name}: {e}")

        logger.info(f"\n{'='*60}")
        logger.info(f"Export completed! Files saved in: {OUTPUT_DIR}/")
        logger.info(f"{'='*60}")

    except Exception as e:
        logger.error(f"Fatal error during export: {e}")
        raise
    finally:
        db_conn.close()
        logger.info("Database connection closed")


def main():
    logger.info("="*60)
    logger.info("SQLite to CSV Export Utility")
    logger.info("="*60)

    if not os.path.exists(DB_PATH):
        logger.error(f"Database file not found: {DB_PATH}")
        return

    export_sqlite_to_csv()


if __name__ == "__main__":
    main()

