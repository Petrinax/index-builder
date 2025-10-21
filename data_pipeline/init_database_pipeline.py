"""Database initialization pipeline - Run on demand to set up database schema"""

import os
from pathlib import Path
from typing import List
from database import DatabaseFactory, DatabaseConnection
from base_logging import Logger

logger = Logger("pipeline.init_database")


class DatabaseInitPipeline:
    """Pipeline for initializing database schema from DDL scripts"""

    def __init__(self, db_type: str = 'sqlite', db_path: str = None):
        """
        Initialize the database setup pipeline

        Args:
            db_type: Type of database ('duckdb' or 'sqlite')
            db_path: Path to database file (optional, will use default if not provided)
        """
        self.db_type = db_type.lower()
        self.db_path = db_path or self._get_default_db_path()
        self.ddl_scripts_dir = Path(__file__).parent / 'ddl_scripts'
        self.db_conn: DatabaseConnection = None

        logger.info(f"Database Init Pipeline initialized for {self.db_type.upper()}: {self.db_path}")

    def _get_default_db_path(self) -> str:
        """Get default database path based on db_type"""
        if self.db_type == 'duckdb':
            return 'stock_data.duckdb'
        elif self.db_type == 'sqlite':
            return 'stock_data.db'
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def _get_ddl_files(self) -> List[Path]:
        """Get all DDL script files in order"""
        # Define order for table creation (to handle any dependencies)
        ordered_files = [
            'create_stock_metadata.sql',
            'create_daily_stock_prices.sql',
            'create_daily_market_cap.sql',
            'create_ingestion_log.sql'
        ]

        ddl_files = []
        for filename in ordered_files:
            file_path = self.ddl_scripts_dir / filename
            if file_path.exists():
                ddl_files.append(file_path)
            else:
                logger.warning(f"DDL file not found: {filename}")

        return ddl_files

    def _read_sql_file(self, file_path: Path) -> str:
        """Read SQL content from a file"""
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            logger.info(f"Read DDL script: {file_path.name}")
            return content
        except Exception as e:
            logger.error(f"Error reading {file_path.name}: {e}")
            raise

    def _execute_ddl_script(self, sql_content: str, script_name: str):
        """Execute DDL script with proper statement splitting"""
        try:
            # Split by semicolon and filter out empty statements
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]

            for statement in statements:
                if statement:
                    self.db_conn.execute(statement)
                    logger.debug(f"Executed statement from {script_name}")

            logger.info(f"Successfully executed DDL script: {script_name}")
        except Exception as e:
            logger.error(f"Error executing {script_name}: {e}")
            raise

    def initialize(self) -> bool:
        """
        Initialize database by executing all DDL scripts

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Connect to database
            self.db_conn = DatabaseFactory.create(self.db_type, self.db_path)
            logger.info(f"Connected to database: {self.db_path}")

            # Get DDL files
            ddl_files = self._get_ddl_files()
            if not ddl_files:
                logger.error("No DDL scripts found!")
                return False

            logger.info(f"Found {len(ddl_files)} DDL scripts to execute")

            # Execute each DDL script
            for ddl_file in ddl_files:
                sql_content = self._read_sql_file(ddl_file)
                self._execute_ddl_script(sql_content, ddl_file.name)

            logger.info(f"Database initialization completed successfully!")
            return True

        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            return False
        finally:
            if self.db_conn:
                self.db_conn.close()
                logger.info("Database connection closed")

    def verify_schema(self) -> bool:
        """
        Verify that all required tables exist

        Returns:
            bool: True if all tables exist, False otherwise
        """
        required_tables = [
            'stock_metadata',
            'daily_stock_prices',
            'daily_market_cap',
            'ingestion_log'
        ]

        try:
            self.db_conn = DatabaseFactory.create(self.db_type, self.db_path)

            if self.db_type == 'duckdb':
                query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            else:  # sqlite
                query = "SELECT name FROM sqlite_master WHERE type='table'"

            self.db_conn.execute(query)
            existing_tables = [row[0] for row in self.db_conn.fetchall()]

            missing_tables = [table for table in required_tables if table not in existing_tables]

            if missing_tables:
                logger.warning(f"Missing tables: {', '.join(missing_tables)}")
                return False

            logger.info("All required tables exist")
            return True

        except Exception as e:
            logger.error(f"Error verifying schema: {e}")
            return False
        finally:
            if self.db_conn:
                self.db_conn.close()


def main():
    """Main entry point for running database initialization"""
    import argparse

    parser = argparse.ArgumentParser(description='Initialize database schema from DDL scripts')
    parser.add_argument('--db-type', choices=['duckdb', 'sqlite'], default='sqlite',
                        help='Database type (default: duckdb)')
    parser.add_argument('--db-path', type=str, help='Path to database file (optional)')
    parser.add_argument('--verify-only', action='store_true',
                        help='Only verify schema without initializing')

    args = parser.parse_args()

    # Create pipeline instance
    pipeline = DatabaseInitPipeline(db_type=args.db_type, db_path=args.db_path)

    if args.verify_only:
        # Only verify schema
        logger.info("Running schema verification...")
        success = pipeline.verify_schema()
    else:
        # Initialize database
        logger.info("Starting database initialization...")
        success = pipeline.initialize()

        # Verify after initialization
        if success:
            logger.info("Verifying schema...")
            pipeline.verify_schema()

    if success:
        logger.info("✓ Database pipeline completed successfully")
        return 0
    else:
        logger.error("✗ Database pipeline failed")
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())

