"""
Data provider client abstraction layer for stock data ingestion.

This module provides a plug-and-play architecture for integrating multiple
financial data providers (Finnhub, Alpha Vantage, IEX Cloud, etc.).
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any, Literal
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv

import pandas as pd

from .base_logging import Logger

load_dotenv()
logger = Logger("data_pipeline.client")


class StockDataClient(ABC):
    """Abstract base class for stock data providers"""

    def __init__(self, api_key: str, rate_limit_delay: float = 1.0):
        self.api_key = api_key
        self.rate_limit_delay = rate_limit_delay
        self._last_request_time = 0

    @abstractmethod
    def get_stock_symbols(self, exchange: str) -> List[Dict[str, Any]]:
        """
        Fetch list of stock symbols for a given exchange.

        Args:
            exchange: Exchange identifier (e.g., 'US', 'NYSE', 'NASDAQ')

        Returns:
            List of dictionaries with stock information:
            [{
                'symbol': str,
                'name': str,
                'exchange': str,
                'mic': str,  # Market Identifier Code
                'currency': str,
                'type': str
            }]
        """
        pass

    @abstractmethod
    def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch real-time/latest quote for a symbol.

        Args:
            symbol: Stock ticker symbol

        Returns:
            Dictionary with quote data:
            {
                'open': float,
                'high': float,
                'low': float,
                'close': float,
                'volume': int,
                'timestamp': datetime
            }
        """
        pass

    @abstractmethod
    def get_company_profile(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch company profile information.

        Args:
            symbol: Stock ticker symbol

        Returns:
            Dictionary with company data:
            {
                'name': str,
                'market_cap': float,
                'shares_outstanding': float,
                'currency': str,
                'country': str,
                'industry': str,
                'sector': str
            }
        """
        pass

    @abstractmethod
    def get_batch_quote(self, symbols: List[str], target_date, period, load_ts) -> pd.DataFrame:
        pass

    def _apply_rate_limit(self):
        """Apply rate limiting between API calls"""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self._last_request_time = time.time()

    def _fetch_with_retry(self, fetch_func, *args, max_retries: int = 3, **kwargs):
        """
        Execute API call with rate limiting and retry logic.

        Args:
            fetch_func: Function to call
            max_retries: Maximum number of retry attempts

        Returns:
            Result from fetch_func or None on failure
        """
        for attempt in range(max_retries):
            try:
                self._apply_rate_limit()
                result = fetch_func(*args, **kwargs)
                return result
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                    time.sleep(self.rate_limit_delay * (attempt + 1))
                else:
                    logger.error(f"All retry attempts failed: {e}")
                    return None
        return None


class FinnhubClient(StockDataClient):
    """Finnhub data provider implementation"""

    def __init__(self, api_key: str = None, rate_limit_delay: float = 1.0):
        api_key = api_key or os.getenv('FINNHUB_API_KEY')
        if not api_key:
            raise ValueError("Finnhub API key is required")

        super().__init__(api_key, rate_limit_delay)

        try:
            import finnhub
            self.client = finnhub.Client(api_key=self.api_key)
            logger.info("Finnhub client initialized")
        except ImportError:
            raise ImportError("finnhub-python package is required. Install with: pip install finnhub-python")

    def get_stock_symbols(self, exchange: str = 'US') -> List[Dict[str, Any]]:
        """Fetch stock symbols from Finnhub"""
        try:
            stocks = self._fetch_with_retry(self.client.stock_symbols, exchange, 'XNYS', 'equities')
            if not stocks:
                return []

            # Normalize to standard format
            normalized = []
            for stock in stocks:
                normalized.append({
                    'symbol': stock.get('symbol', ''),
                    'name': stock.get('description', ''),
                    'exchange': exchange,
                    'mic': stock.get('mic', ''),
                    'currency': stock.get('currency', 'USD'),
                    'type': stock.get('type', 'Common Stock')
                })

            logger.info(f"Fetched {len(normalized)} symbols from Finnhub for {exchange}")
            return normalized
        except Exception as e:
            logger.error(f"Error fetching symbols from Finnhub: {e}")
            return []

    def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch quote from Finnhub"""
        try:
            quote = self._fetch_with_retry(self.client.quote, symbol)
            if not quote or quote.get('c') == 0:
                return None

            # Normalize to standard format
            return {
                'open': quote.get('o'),
                'high': quote.get('h'),
                'low': quote.get('l'),
                'close': quote.get('c'),
                'previous_close': quote.get('pc'),
                'volume': quote.get('v'),
                'timestamp': datetime.fromtimestamp(quote.get('t', time.time()))
            }
        except Exception as e:
            logger.warning(f"Error fetching quote for {symbol}: {e}")
            return None
    def get_batch_quote(self, symbols: List[str], target_date, period, load_ts) -> pd.DataFrame:
        raise NotImplementedError("Batch quote fetching not available for FinnhubClient")

    def get_company_profile(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch company profile from Finnhub"""
        try:
            profile = self._fetch_with_retry(self.client.company_profile2, symbol=symbol)
            if not profile:
                return None

            # Normalize to standard format
            return {
                'name': profile.get('name', ''),
                'market_cap': profile.get('marketCapitalization'),
                'shares_outstanding': profile.get('shareOutstanding'),
                'currency': profile.get('currency', 'USD'),
                'country': profile.get('country', ''),
                'industry': profile.get('finnhubIndustry', ''),
                'sector': profile.get('finnhubIndustry', ''),
                'ipo_date': profile.get('ipo'),
                'logo': profile.get('logo', ''),
                'phone': profile.get('phone', ''),
                'weburl': profile.get('weburl', '')
            }
        except Exception as e:
            logger.warning(f"Error fetching profile for {symbol}: {e}")
            return None


class YFinanceClient(StockDataClient):
    """Yahoo Finance data provider implementation"""

    def __init__(self, api_key: str = None, rate_limit_delay: float = 0.5):
        # yfinance doesn't require an API key
        super().__init__(api_key or 'not_required', rate_limit_delay)

        try:
            import yfinance as yf
            self.yf = yf
            logger.info("YFinance client initialized")
        except ImportError:
            raise ImportError("yfinance package is required. Install with: pip install yfinance")

    def get_stock_symbols(self, exchange: str = 'US') -> List[Dict[str, Any]]:
        """
        Fetch stock symbols from Yahoo Finance.
        Note: yfinance doesn't provide a direct symbol listing API.

        Alternate: Fetches NYSE symbols from NASDAQ Trader website.
        """

        if exchange in ['US', 'NYSE']:
            import pandas as pd

            url = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
            df = pd.read_csv(url, sep="|")
            nyse_df = df[df["Exchange"] == "N"]

            # Rename columns to match our standard format and add constants
            nyse_df = nyse_df.rename(columns={
                'ACT Symbol': 'symbol',
                'Security Name': 'name'
            })
            nyse_df['exchange'] = 'NYSE'
            nyse_df['mic'] = 'XNYS'
            nyse_df['currency'] = 'USD'
            nyse_df['type'] = 'Common Stock'

            # Convert to list of dicts with only the columns we need
            normalized = nyse_df[['symbol', 'name', 'exchange', 'mic', 'currency', 'type']].to_dict('records')

            logger.info(f"Fetched {len(normalized)} symbols from NASDAQ Trader for NYSE")
            return normalized
        else:
            logger.warning("YFinance doesn't provide symbol listing. Use Finnhub or maintain a static list.")
            return []

    def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch quote from Yahoo Finance"""
        try:
            ticker = self.yf.Ticker(symbol)

            # Get the most recent data
            hist = ticker.history(period='1d')

            if hist.empty:
                logger.warning(f"No quote data available for {symbol}")
                return None

            # Get the latest row
            latest = hist.iloc[-1]

            # Get previous close from info if available
            info = ticker.info
            previous_close = info.get('previousClose', latest.get('Close'))

            # Normalize to standard format
            return {
                'open': float(latest['Open']) if 'Open' in latest else None,
                'high': float(latest['High']) if 'High' in latest else None,
                'low': float(latest['Low']) if 'Low' in latest else None,
                'close': float(latest['Close']) if 'Close' in latest else None,
                'previous_close': float(previous_close) if previous_close else None,
                'volume': int(latest['Volume']) if 'Volume' in latest else None,
                'timestamp': latest.name.to_pydatetime() if hasattr(latest.name, 'to_pydatetime') else datetime.now()
            }
        except Exception as e:
            logger.warning(f"Error fetching quote for {symbol}: {e}")
            return None

    def get_batch_quote(self, symbols: List[str], target_date: str, period: str, load_ts: datetime) -> pd.DataFrame:
        """
        Fetch batch quotes for multiple symbols in batches of 500.

        Args:
            symbols: List of stock symbols to fetch
            target_date: Target date for the quotes (not used for yfinance, defaults to latest)
            period: Period for historical data.
                Valid periods: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd

        Returns:
            DataFrame with combined results from all batches
            :param load_ts:
        """
        batch_size = 500
        all_results = []
        query_params: Dict[Any, Any] = {
            'interval': '1d',
            'group_by': 'ticker',
            'threads': True,
            'auto_adjust': True
        }
        if target_date:
            query_params['start'] = datetime.strptime(target_date, '%Y-%m-%d')
            query_params['end'] = query_params['start'] + timedelta(days=1)
        else:
            query_params['period'] = period or '1d'

        # Define database columns to keep
        db_columns = ['symbol', 'exchange', 'mic', 'open', 'high', 'low', 'close', 'volume', 'date', 'last_updated']

        # Split symbols into batches of 500
        for i in range(0, len(symbols), batch_size):
            time.sleep(3) # To respect rate limits
            batch = symbols[i:i + batch_size]
            logger.info(f"Fetching batch {i//batch_size + 1} of {(len(symbols)-1)//batch_size + 1} ({len(batch)} symbols)")

            try:
                data = self.yf.download(tickers=batch, **query_params)

                if data.empty:
                    logger.warning(f"No data returned for batch {i//batch_size + 1}")
                    continue

                # Handle single symbol vs multiple symbols differently
                if len(batch) == 1:
                    # For single symbol, data structure is different
                    flat_df = data.copy()
                    flat_df['symbol'] = batch[0]
                    flat_df.reset_index(inplace=True)
                    flat_df.rename(columns={'Date': 'date'}, inplace=True)
                else:
                    # For multiple symbols, use stack
                    flat_df = (
                        data.stack(level=0, future_stack=True)
                        .rename_axis(index=["date", "symbol"])
                        .reset_index()
                    )

                flat_df['exchange'] = 'NYSE'
                flat_df['mic'] = 'XNYS'
                flat_df['last_updated'] = load_ts
                flat_df.rename(columns={
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Volume': 'volume'
                }, inplace=True)

                # Convert date column to '%Y-%m-%d' format
                if 'date' in flat_df.columns:
                    flat_df['date'] = pd.to_datetime(flat_df['date']).dt.strftime('%Y-%m-%d')

                # Keep only database columns, drop any extra columns from yfinance
                available_columns = [col for col in db_columns if col in flat_df.columns]
                flat_df = flat_df[available_columns]

                all_results.append(flat_df)
                logger.info(f"Successfully fetched {len(flat_df)} records for batch {i//batch_size + 1}")

            except Exception as e:
                logger.error(f"Error fetching batch {i//batch_size + 1}: {e}")
                continue

        # Combine all batch results
        if not all_results:
            logger.warning("No data retrieved from any batch")
            return pd.DataFrame()

        combined_df = pd.concat(all_results, ignore_index=True)
        logger.info(f"Total records fetched: {len(combined_df)} across {len(all_results)} batches")

        return combined_df



    def get_company_profile(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch company profile from Yahoo Finance.
        Calculates market cap if not directly available using shares_outstanding * close price.
        """
        try:
            ticker = self.yf.Ticker(symbol)
            info = ticker.info

            if not info or len(info) == 0:
                logger.warning(f"No profile data available for {symbol}")
                return None

            shares_outstanding = info.get('sharesOutstanding')
            current_price = info.get('currentPrice') or info.get('regularMarketPrice')
            market_cap = shares_outstanding * current_price

            # # Try to get market cap directly
            # market_cap = info.get('marketCap')
            #
            # # If market cap not available, calculate it
            # if not market_cap or market_cap == 0:
            #     shares_outstanding = info.get('sharesOutstanding')
            #     current_price = info.get('currentPrice') or info.get('regularMarketPrice')
            #
            #     if shares_outstanding and current_price:
            #         market_cap = shares_outstanding * current_price
            #         logger.info(f"Calculated market cap for {symbol}: {market_cap}")
            #     else:
            #         logger.warning(f"Cannot calculate market cap for {symbol}: missing shares or price data")
            #         market_cap = None

            # Normalize to standard format
            return {
                'name': info.get('longName') or info.get('shortName', ''),
                'market_cap': market_cap,
                'shares_outstanding': info.get('sharesOutstanding'),
                'currency': info.get('currency', 'USD'),
                'country': info.get('country', ''),
                'industry': info.get('industry', ''),
                'sector': info.get('sector', ''),
                'ipo_date': None,  # yfinance doesn't always provide this
                'logo': info.get('logo_url', ''),
                'phone': info.get('phone', ''),
                'weburl': info.get('website', '')
            }
        except Exception as e:
            logger.warning(f"Error fetching profile for {symbol}: {e}")
            return None


class AlphaVantageClient(StockDataClient):
    """Alpha Vantage data provider implementation (ready for implementation)"""

    def __init__(self, api_key: str = None, rate_limit_delay: float = 12.0):
        # Alpha Vantage free tier: 5 calls per minute
        api_key = api_key or os.getenv('ALPHA_VANTAGE_API_KEY')
        if not api_key:
            raise ValueError("Alpha Vantage API key is required")

        super().__init__(api_key, rate_limit_delay)
        logger.info("Alpha Vantage client initialized")

    def get_stock_symbols(self, exchange: str = 'US') -> List[Dict[str, Any]]:
        """Fetch stock symbols from Alpha Vantage"""
        # TODO: Implement Alpha Vantage symbol listing
        # Alpha Vantage doesn't have a direct symbol list endpoint
        # May need to use a static list or alternative source
        logger.warning("Alpha Vantage symbol listing not yet implemented")
        return []

    def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch quote from Alpha Vantage"""
        # TODO: Implement using GLOBAL_QUOTE endpoint
        logger.warning("Alpha Vantage quote not yet implemented")
        return None

    def get_company_profile(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch company overview from Alpha Vantage"""
        # TODO: Implement using OVERVIEW endpoint
        logger.warning("Alpha Vantage company profile not yet implemented")
        return None


class IEXCloudClient(StockDataClient):
    """IEX Cloud data provider implementation (ready for implementation)"""

    def __init__(self, api_key: str = None, rate_limit_delay: float = 0.1):
        api_key = api_key or os.getenv('IEX_CLOUD_API_KEY')
        if not api_key:
            raise ValueError("IEX Cloud API key is required")

        super().__init__(api_key, rate_limit_delay)
        self.base_url = os.getenv('IEX_CLOUD_BASE_URL', 'https://cloud.iexapis.com/stable')
        logger.info("IEX Cloud client initialized")

    def get_stock_symbols(self, exchange: str = 'US') -> List[Dict[str, Any]]:
        """Fetch stock symbols from IEX Cloud"""
        # TODO: Implement using /ref-data/symbols endpoint
        logger.warning("IEX Cloud symbol listing not yet implemented")
        return []

    def get_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch quote from IEX Cloud"""
        # TODO: Implement using /stock/{symbol}/quote endpoint
        logger.warning("IEX Cloud quote not yet implemented")
        return None

    def get_company_profile(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch company info from IEX Cloud"""
        # TODO: Implement using /stock/{symbol}/company endpoint
        logger.warning("IEX Cloud company profile not yet implemented")
        return None


class ClientFactory:
    """Factory for creating data provider clients"""

    _client_registry = {
        'finnhub': FinnhubClient,
        'alphavantage': AlphaVantageClient,
        'iexcloud': IEXCloudClient,
        'yfinance': YFinanceClient,
    }

    @classmethod
    def create(cls, provider: str, api_key: str = None, rate_limit_delay: float = None) -> StockDataClient:
        """
        Create a data provider client.

        Args:
            provider: Provider name ('finnhub', 'alphavantage', 'iexcloud')
            api_key: API key (optional, will use env vars if not provided)
            rate_limit_delay: Delay between API calls in seconds

        Returns:
            StockDataClient instance

        Raises:
            ValueError: If provider is not supported
        """
        provider = provider.lower()

        if provider not in cls._client_registry:
            available = ', '.join(cls._client_registry.keys())
            raise ValueError(f"Unsupported provider: {provider}. Available: {available}")

        client_class = cls._client_registry[provider]

        # Create client with optional parameters
        kwargs = {}
        if api_key:
            kwargs['api_key'] = api_key
        if rate_limit_delay is not None:
            kwargs['rate_limit_delay'] = rate_limit_delay

        return client_class(**kwargs)

    @classmethod
    def register_client(cls, name: str, client_class: type):
        """
        Register a new client provider.

        Args:
            name: Provider name
            client_class: Class implementing StockDataClient
        """
        if not issubclass(client_class, StockDataClient):
            raise TypeError("Client class must inherit from StockDataClient")

        cls._client_registry[name.lower()] = client_class
        logger.info(f"Registered new client provider: {name}")

    @classmethod
    def list_providers(cls) -> List[str]:
        """Get list of available providers"""
        return list(cls._client_registry.keys())
