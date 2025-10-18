"""Database abstraction layer for stock data ingestion"""

import duckdb
import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List, Tuple


class DatabaseConnection(ABC):
    """Abstract base class for database connections"""

    @abstractmethod
    def connect(self, db_path: str):
        """Establish database connection"""
        pass

    @abstractmethod
    def execute(self, query: str, params: List = None):
        """Execute a query"""
        pass

    @abstractmethod
    def fetchone(self) -> Tuple:
        """Fetch one result"""
        pass

    @abstractmethod
    def fetchall(self) -> List[Tuple]:
        """Fetch all results"""
        pass

    @abstractmethod
    def close(self):
        """Close database connection"""
        pass

    def execute_sql_file(self, file_path: str):
        """
        Execute SQL statements from a file

        Args:
            file_path: Path to SQL file
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"SQL file not found: {file_path}")

        with open(path, 'r') as f:
            sql_content = f.read()

        # Split by semicolon and execute each statement
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        for statement in statements:
            if statement:
                self.execute(statement)


class DuckDBConnection(DatabaseConnection):
    """DuckDB implementation"""

    def __init__(self):
        self.conn = None
        self._last_result = None

    def connect(self, db_path: str):
        self.conn = duckdb.connect(db_path)
        return self

    def execute(self, query: str, params: List = None):
        if params:
            self._last_result = self.conn.execute(query, params)
        else:
            self._last_result = self.conn.execute(query)
        return self

    def fetchone(self) -> Tuple:
        return self._last_result.fetchone() if self._last_result else None

    def fetchall(self) -> List[Tuple]:
        return self._last_result.fetchall() if self._last_result else []

    def close(self):
        if self.conn:
            self.conn.close()


class SQLiteConnection(DatabaseConnection):
    """SQLite implementation"""

    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        return self

    def execute(self, query: str, params: List = None):
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)
        self.conn.commit()
        return self

    def fetchone(self) -> Tuple:
        return self.cursor.fetchone() if self.cursor else None

    def fetchall(self) -> List[Tuple]:
        return self.cursor.fetchall() if self.cursor else []

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


class DatabaseFactory:
    """Factory for creating database connections"""

    @staticmethod
    def create(db_type: str, db_path: str) -> DatabaseConnection:
        """
        Create database connection

        Args:
            db_type: 'duckdb' or 'sqlite'
            db_path: Path to database file

        Returns:
            DatabaseConnection instance
        """
        db_type = db_type.lower()

        if db_type == 'duckdb':
            return DuckDBConnection().connect(db_path)
        elif db_type == 'sqlite':
            return SQLiteConnection().connect(db_path)
        else:
            raise ValueError(f"Unsupported database type: {db_type}. Use 'duckdb' or 'sqlite'")
