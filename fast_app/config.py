"""
Configuration settings for the FastAPI application
"""

import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

# Load environment variables
load_dotenv(find_dotenv())

class Settings:
    """Application settings"""

    # Database configuration
    DB_TYPE: str = os.getenv('DB_TYPE', 'sqlite')
    DB_PATH: str = os.getenv('DB_PATH', str(Path(__file__).parent.parent / 'data' / 'stock_data.db'))
    DEBUG: bool = os.getenv('DEBUG', 'FALSE').upper() == 'TRUE'

    # Index configuration
    DEFAULT_TOP_N: int = 100
    DEFAULT_INITIAL_NAV: float = 1000.0

    # Cache configuration
    CACHE_ENABLED: bool = True

    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
    REDIS_TTL: int = int(os.getenv("REDIS_TTL", "600"))  # 10 minutes default

    # Export configuration
    EXPORT_DIR: str = os.getenv('EXPORT_DIR', str(Path(__file__).parent / 'exports'))

    def __init__(self):
        # Create export directory if it doesn't exist
        os.makedirs(self.EXPORT_DIR, exist_ok=True)


settings = Settings()

