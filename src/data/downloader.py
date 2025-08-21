"""
Financial data downloader for multiple sources (Polygon.io)
"""
import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()




class DataDownloader:
    """Downloads financial data from Polygon.io (OHLCV and Corporate Events)"""

    def __init__(self):
        # Polygon.io setup
        self.polygon_api_key = os.getenv('POLYGON_API_KEY')
        if not self.polygon_api_key:
            print("Warning: POLYGON_API_KEY not set. Polygon.io features will be disabled.")

        self.polygon_base_url = "https://api.polygon.io"
        self.polygon_rate_limit = 0.1  # 100ms between requests

        # Corporate events output directory
        self.corporate_events_output_dir = "data/corporate_events"
        self._ensure_corporate_events_directory()

        # Stock universe setup
        self.universe_rate_limit = 0.5  # 500ms between requests

    def _ensure_corporate_events_directory(self):
        """Create corporate events output directory if it doesn't exist"""
        os.makedirs(self.corporate_events_output_dir, exist_ok=True)

    # Stock Universe Methods
    def _scrape_wikipedia_table(self, url: str, table_index: int = 0) -> List[str]:
        """Scrape symbols from a Wikipedia table"""
        try:
            print(f"Scraping from: {url}")

            # Get the Wikipedia page
            response = requests.get(url)
            response.raise_for_status()

            # Parse HTML tables
            tables = pd.read_html(response.text)

            if not tables or table_index >= len(tables):
                print(f"No tables found or table index {table_index} out of range")
                return []

            table = tables[table_index]
            print(f"Found table with shape: {table.shape}")
            print(f"Table columns: {table.columns.tolist()}")

            # Look for symbol/ticker column
            symbol_columns = ['Symbol', 'Ticker', 'Ticker Symbol', 'Symbol/Ticker']
            symbol_col = None

            for col in symbol_columns:
                if col in table.columns:
                    symbol_col = col
                    break

            if symbol_col is None:
                # Try to find column containing symbols
                for col in table.columns:
                    if table[col].dtype == 'object':
                        # Check if first few values look like stock symbols
                        sample_values = table[col].dropna().head(10).astype(str)
                        if all(len(str(val)) <= 5 and str(val).isupper() for val in sample_values):
                            symbol_col = col
                            break

            if symbol_col is None:
                print(f"Could not identify symbol column. Available columns: {table.columns.tolist()}")
                print(f"First few rows of first column: {table.iloc[:5, 0].tolist()}")
                return []

            # Extract symbols
            symbols = table[symbol_col].dropna().astype(str).tolist()

            # Clean symbols (remove any non-alphanumeric characters except dots)
            cleaned_symbols = []
            for symbol in symbols:
                cleaned = ''.join(c for c in symbol if c.isalnum() or c == '.')
                if cleaned and len(cleaned) <= 5:  # Most stock symbols are 1-5 characters
                    cleaned_symbols.append(cleaned)

            # Remove duplicates and sort
            unique_symbols = sorted(list(set(cleaned_symbols)))

            print(f"Extracted {len(unique_symbols)} unique symbols from {symbol_col} column")
            print(f"Sample symbols: {unique_symbols[:10]}")

            return unique_symbols

        except Exception as e:
            print(f"Error scraping Wikipedia: {e}")
            return []

    def get_sp500_symbols(self) -> List[str]:
        """Get S&P 500 symbols from Wikipedia"""
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        symbols = self._scrape_wikipedia_table(url)

        if len(symbols) >= 400:  # Should be around 500
            print(f"✓ Successfully scraped {len(symbols)} S&P 500 symbols from Wikipedia")
            return symbols
        else:
            print(f"⚠️ Only got {len(symbols)} symbols, using fallback list")
            return self._get_sp500_fallback()

    def _get_sp500_fallback(self) -> List[str]:
        """Fallback S&P 500 list if Wikipedia scraping fails"""
        return [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'BRK.B', 'LLY', 'V', 'UNH',
            'JPM', 'JNJ', 'PG', 'HD', 'MA', 'PFE', 'ABBV', 'KO', 'PEP', 'TMO',
            'COST', 'AVGO', 'MRK', 'WMT', 'BAC', 'ACN', 'DHR', 'VZ', 'ADBE', 'CRM'
        ]

    # OHLCV Data Methods (from Polygon.io)
    def download_ohlcv_data(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        Download daily OHLCV data for a single stock from Polygon.io

        Args:
            symbol: Stock ticker symbol
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            DataFrame with columns: date, open, high, low, close, volume
        """
        if not self.polygon_api_key:
            print(f"Error: POLYGON_API_KEY not set. Cannot download OHLCV data for {symbol}")
            return None

        try:
            url = f"{self.polygon_base_url}/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
            params = {
                'apiKey': self.polygon_api_key,
                'adjusted': 'true',
                'sort': 'asc'
            }

            response = requests.get(url, params=params)
            response.raise_for_status()

            data = response.json()

            if data['status'] != 'OK' or not data['results']:
                print(f"No data found for {symbol}")
                return None

            # Convert to DataFrame
            df = pd.DataFrame(data['results'])
            df['date'] = pd.to_datetime(df['t'], unit='ms').dt.date
            df = df[['date', 'o', 'h', 'l', 'c', 'v']]
            df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']

            print(f"✓ Downloaded {len(df)} daily OHLCV records for {symbol}")

            # Rate limiting
            time.sleep(self.polygon_rate_limit)

            return df

        except requests.exceptions.RequestException as e:
            print(f"Request error downloading OHLCV data for {symbol}: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error downloading OHLCV data for {symbol}: {e}")
            return None

    def download_multiple_ohlcv(self, symbols: List[str], start_date: str, end_date: str) -> dict:
        """
        Download OHLCV data for multiple stocks from Polygon.io

        Args:
            symbols: List of stock tickers
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            Dictionary with symbol as key and DataFrame as value
        """
        results = {}

        for symbol in symbols:
            print(f"Downloading OHLCV data for {symbol}...")
            data = self.download_ohlcv_data(symbol, start_date, end_date)
            if data is not None:
                results[symbol] = data
                print(f"Successfully downloaded {len(data)} days of OHLCV data for {symbol}")
            else:
                print(f"Failed to download OHLCV data for {symbol}")

        return results

    def save_ohlcv_data(self, data_dict: dict, output_dir: str = "data/raw"):
        """
        Save downloaded OHLCV data to CSV files

        Args:
            data_dict: Dictionary of DataFrames from download_multiple_ohlcv
            output_dir: Directory to save CSV files
        """
        os.makedirs(output_dir, exist_ok=True)

        for symbol, df in data_dict.items():
            filename = f"{output_dir}/{symbol}_daily.csv"
            df.to_csv(filename, index=False)
            print(f"Saved {symbol} OHLCV data to {filename}")

    # Corporate Events Methods (from Polygon.io TMX)
    def download_corporate_events(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        Download corporate events data for a single stock from Polygon.io TMX using the official Python client

        Args:
            symbol: Stock ticker symbol

        Returns:
            DataFrame with corporate events data or None if failed
        """
        if not self.polygon_api_key:
            print(f"Error: POLYGON_API_KEY not set. Cannot download corporate events for {symbol}")
            return None

        try:
            print(f"Downloading corporate events for {symbol}...")

            # Import the official Polygon.io client
            try:
                from polygon import RESTClient
            except ImportError:
                print("Error: 'polygon' package not installed. Please install it with: pip install polygon-api-client")
                return None

            # Initialize the REST client
            client = RESTClient(self.polygon_api_key)

            # Use the official client to get corporate events
            corporate_events = []

            try:
                # Get corporate events using the official client
                events = client.list_tmx_corporate_events(
                    ticker=symbol,
                    limit=50000
                )

                # Process corporate events data
                for event in events:
                    event_entry = {
                        'date': getattr(event, 'date', None),
                        'ticker': getattr(event, 'ticker', symbol),
                        'company_name': getattr(event, 'company_name', None),
                        'event_type': getattr(event, 'type', None),
                        'event_name': getattr(event, 'name', None),
                        'status': getattr(event, 'status', None),
                        'trading_venue': getattr(event, 'trading_venue', None),
                        'isin': getattr(event, 'isin', None),
                        'tmx_company_id': getattr(event, 'tmx_company_id', None),
                        'tmx_record_id': getattr(event, 'tmx_record_id', None),
                        'url': getattr(event, 'url', None)
                    }

                    corporate_events.append(event_entry)

            except Exception as e:
                print(f"Error retrieving corporate events from client: {e}")
                return None

            # Convert to DataFrame and sort by date (most recent first)
            if corporate_events:
                df = pd.DataFrame(corporate_events)
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date', ascending=False)
                print(f"✓ Downloaded {len(df)} corporate events for {symbol}")
            else:
                df = pd.DataFrame()
                print(f"✓ No corporate events found for {symbol}")

            # Rate limiting
            time.sleep(self.polygon_rate_limit)

            return df

        except Exception as e:
            print(f"Error downloading corporate events for {symbol}: {e}")
            return None

    def download_multiple_events(self, symbols: List[str]) -> pd.DataFrame:
        """
        Download corporate events data for multiple stocks from Polygon.io TMX

        Args:
            symbols: List of stock tickers

        Returns:
            DataFrame with corporate events data and a symbols column
        """
        all_events = []
        successful_downloads = 0
        failed_downloads = 0

        print(f"Starting corporate events download for {len(symbols)} stocks")
        print("=" * 60)

        for i, symbol in enumerate(symbols, 1):
            print(f"[{i}/{len(symbols)}] Processing {symbol}...")

            events = self.download_corporate_events(symbol)

            if events is not None and not events.empty:
                # Add symbol column to each event
                events_copy = events.copy()
                events_copy['symbol'] = symbol
                all_events.append(events_copy)
                successful_downloads += 1

                filename = f"corporate_events_{symbol}.csv"
                filepath = os.path.join("data/corporate_events", filename)

                try:
                    events_copy.to_csv(filepath, index=False)
                    print(f"{symbol}: Corporate events saved to: {filepath}")
                except Exception as e:
                    print(f"Warning: Could not save CSV file: {e}")


            else:
                failed_downloads += 1
                print(f"✗ Failed to download corporate events for {symbol}")

        print("=" * 60)
        print(f"Download Summary:")
        print(f"✓ Successful: {successful_downloads}")
        print(f"✗ Failed: {failed_downloads}")
        print(f"Total processed: {len(symbols)}")

        # Combine all DataFrames
        if all_events:
            combined_df = pd.concat(all_events, ignore_index=True)
            print(f"Total events: {len(combined_df)}")
            return combined_df
        else:
            return pd.DataFrame()

    def download_all_events(self, market: str = "stocks", active: bool = True) -> pd.DataFrame:
        """
        Download corporate events data for all tickers supported by Polygon.io

        Args:
            market: Market type (default: "stocks")
            active: Whether to return only active tickers (default: True)

        Returns:
            DataFrame with corporate events data for all tickers
        """
        try:
            print(f"Starting download of corporate events for all {market} tickers...")

            # Get all available ticker symbols
            print("Retrieving all available ticker symbols...")
            all_tickers = self.get_polygon_ticker_symbols(market, active)

            if not all_tickers:
                print("No ticker symbols found")
                return pd.DataFrame()

            print(f"Processing {len(all_tickers)} tickers for corporate events...")

            # Use the existing download_multiple_events function
            combined_df = self.download_multiple_events(all_tickers)

            # if not combined_df.empty:
            #     # Save to CSV
            #     self._ensure_corporate_events_directory()
            #     timestamp = datetime.now().strftime("%Y%m%d")
            #     filename = f"all_corporate_events_{market}_{timestamp}.csv"
            #     filepath = os.path.join("data/corporate_events", filename)

            #     try:
            #         combined_df.to_csv(filepath, index=False)
            #         print(f"✓ All corporate events saved to: {filepath}")
            #     except Exception as e:
            #         print(f"Warning: Could not save CSV file: {e}")

            #     return combined_df
            # else:
            #     print("No corporate events found for any tickers")
            #     return pd.DataFrame()

        except Exception as e:
            print(f"Error downloading all corporate events: {e}")
            return pd.DataFrame()

    def download_fundamentals(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        Download fundamental data for a single stock from Polygon.io using the official Python client

        Args:
            symbol: Stock ticker symbol

        Returns:
            DataFrame with fundamental data or None if failed
        """
        if not self.polygon_api_key:
            print(f"Error: POLYGON_API_KEY not set. Cannot download fundamentals for {symbol}")
            return None

        try:
            print(f"Downloading fundamental data for {symbol}...")

            # Import the official Polygon.io client
            try:
                from polygon import RESTClient
            except ImportError:
                print("Error: 'polygon' package not installed. Please install it with: pip install polygon-api-client")
                return None

            # Initialize the REST client
            client = RESTClient(self.polygon_api_key)

            # Use the official client to get fundamental data
            fundamentals = []

            try:
                # Get fundamental data using the official client
                financials = client.vx.list_stock_financials(
                    ticker=symbol,
                    limit=1000
                )

                # Process fundamental data
                for financial in financials:
                    fundamental_entry = {
                        'ticker': getattr(financial, 'ticker', symbol),
                        'period_of_report_date': getattr(financial, 'period_of_report_date', None),
                        'filing_date': getattr(financial, 'filing_date', None),
                        'source_filing_file_url': getattr(financial, 'source_filing_file_url', None),
                        'source_filing_url': getattr(financial, 'source_filing_url', None),
                        'period_length': getattr(financial, 'period_length', None),
                        'period_end_date': getattr(financial, 'period_end_date', None),
                        'fiscal_period': getattr(financial, 'fiscal_period', None),
                        'fiscal_year': getattr(financial, 'fiscal_year', None),
                        'cik': getattr(financial, 'cik', None),
                        'company_name': getattr(financial, 'company_name', None),
                        'sic_code': getattr(financial, 'sic_code', None),
                        'sic_description': getattr(financial, 'sic_description', None),
                        'naics_code': getattr(financial, 'naics_code', None),
                        'naics_description': getattr(financial, 'naics_description', None),
                        'filing_url': getattr(financial, 'filing_url', None),
                        'filing_title': getattr(financial, 'filing_title', None),
                        'filing_date_str': getattr(financial, 'filing_date_str', None),
                        'period_of_report_date_str': getattr(financial, 'period_of_report_date_str', None),
                        'period_end_date_str': getattr(financial, 'period_end_date_str', None),
                        'filing_href': getattr(financial, 'filing_href', None),
                        'financials': getattr(financial, 'financials', None)
                    }

                    fundamentals.append(fundamental_entry)

            except Exception as e:
                print(f"Error retrieving fundamentals from client: {e}")
                return None

            # Convert to DataFrame and sort by period end date (most recent first)
            if fundamentals:
                df = pd.DataFrame(fundamentals)
                # Convert date columns to datetime for sorting
                date_columns = ['period_of_report_date', 'filing_date', 'period_end_date']
                for col in date_columns:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce')

                # Sort by period end date (most recent first)
                if 'period_end_date' in df.columns:
                    df = df.sort_values('period_end_date', ascending=False, na_position='last')

                print(f"✓ Downloaded {len(df)} fundamental records for {symbol}")
            else:
                df = pd.DataFrame()
                print(f"✓ No fundamental data found for {symbol}")

            # Rate limiting
            time.sleep(self.polygon_rate_limit)

            return df

        except Exception as e:
            print(f"Error downloading fundamental data for {symbol}: {e}")
            return None

    def download_multiple_fundamentals(self, symbols: List[str]) -> pd.DataFrame:
        """
        Download fundamental data for multiple stocks from Polygon.io

        Args:
            symbols: List of stock tickers

        Returns:
            DataFrame with fundamental data and a symbols column
        """
        all_fundamentals = []
        successful_downloads = 0
        failed_downloads = 0

        print(f"Starting fundamental data download for {len(symbols)} stocks")
        print("=" * 60)

        for i, symbol in enumerate(symbols, 1):
            print(f"[{i}/{len(symbols)}] Processing {symbol}...")

            fundamentals = self.download_fundamentals(symbol)

            if fundamentals is not None and not fundamentals.empty:
                # Add symbol column to each fundamental record
                fundamentals_copy = fundamentals.copy()
                fundamentals_copy['symbol'] = symbol
                all_fundamentals.append(fundamentals_copy)
                successful_downloads += 1
                print(f"  ✓ Found {len(fundamentals)} fundamental records")
            else:
                failed_downloads += 1
                print(f"  ✗ No fundamental data found")

        print("=" * 60)
        print(f"Download Summary:")
        print(f"✓ Successful: {successful_downloads}")
        print(f"✗ Failed: {failed_downloads}")
        print(f"Total processed: {len(symbols)}")

        # Combine all DataFrames
        if all_fundamentals:
            combined_df = pd.concat(all_fundamentals, ignore_index=True)
            print(f"Total fundamental records: {len(combined_df)}")
            return combined_df
        else:
            return pd.DataFrame()

    def download_all_fundamentals(self, market: str = "stocks", active: bool = True) -> pd.DataFrame:
        """
        Download fundamental data for all tickers supported by Polygon.io

        Args:
            market: Market type (default: "stocks")
            active: Whether to return only active tickers (default: True)

        Returns:
            DataFrame with fundamental data for all tickers
        """
        try:
            print(f"Starting download of fundamental data for all {market} tickers...")

            # Get all available ticker symbols
            print("Retrieving all available ticker symbols...")
            all_tickers = self.get_polygon_ticker_symbols(market, active)

            if not all_tickers:
                print("No ticker symbols found")
                return pd.DataFrame()

            print(f"Processing {len(all_tickers)} tickers for fundamental data...")

            # Use the existing download_multiple_fundamentals function
            combined_df = self.download_multiple_fundamentals(all_tickers)

            if not combined_df.empty:
                # Save to CSV
                self._ensure_fundamentals_directory()
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"all_fundamentals_{market}_{timestamp}.csv"
                filepath = os.path.join("data/fundamentals", filename)

                try:
                    combined_df.to_csv(filepath, index=False)
                    print(f"✓ All fundamental data saved to: {filepath}")
                except Exception as e:
                    print(f"Warning: Could not save CSV file: {e}")

                return combined_df
            else:
                print("No fundamental data found for any tickers")
                return pd.DataFrame()

        except Exception as e:
            print(f"Error downloading all fundamental data: {e}")
            return pd.DataFrame()

    def _ensure_fundamentals_directory(self):
        """Create fundamentals output directory if it doesn't exist"""
        os.makedirs("data/fundamentals", exist_ok=True)

    def get_polygon_tickers_all(self, market: str = "stocks", active: bool = True, limit: int = 1000) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieve a complete list of tickers supported by Polygon.io using the official Python client

        Args:
            market: Market type (default: "stocks")
            active: Whether to return only active tickers (default: True)
            limit: Maximum number of results per request (default: 1000, max: 1000)

        Returns:
            List of ticker dictionaries or None if failed
        """
        if not self.polygon_api_key:
            print("Error: POLYGON_API_KEY not set. Cannot retrieve tickers from Polygon.io")
            return None

        try:
            print(f"Retrieving all {market} tickers from Polygon.io...")

            # Import the official Polygon.io client
            try:
                from polygon import RESTClient
            except ImportError:
                print("Error: 'polygon' package not installed. Please install it with: pip install polygon-api-client")
                return None

            # Initialize the REST client
            client = RESTClient(self.polygon_api_key)

            all_tickers = []

            # Use the official client to iterate through all tickers
            for ticker in client.list_tickers(
                market=market,
                active=str(active).lower(),
                order="asc",
                limit=str(limit),
                sort="ticker"
            ):
                # Convert ticker object to dictionary
                ticker_dict = {
                    'ticker': ticker.ticker,
                    'name': getattr(ticker, 'name', None),
                    'market': getattr(ticker, 'market', None),
                    'locale': getattr(ticker, 'locale', None),
                    'primary_exchange': getattr(ticker, 'primary_exchange', None),
                    'type': getattr(ticker, 'type', None),
                    'active': getattr(ticker, 'active', None),
                    'currency_name': getattr(ticker, 'currency_name', None),
                    'cik': getattr(ticker, 'cik', None),
                    'composite_figi': getattr(ticker, 'composite_figi', None),
                    'share_class_figi': getattr(ticker, 'share_class_figi', None),
                    'last_updated_utc': getattr(ticker, 'last_updated_utc', None)
                }
                all_tickers.append(ticker_dict)

                # Progress indicator for large datasets
                if len(all_tickers) % 1000 == 0:
                    print(f"  Retrieved {len(all_tickers)} tickers...")

            print(f"✓ Successfully retrieved {len(all_tickers)} total tickers")
            return all_tickers

        except Exception as e:
            print(f"Error retrieving tickers from Polygon.io: {e}")
            return None

    def get_polygon_ticker_symbols(self, market: str = "stocks", active: bool = True) -> Optional[List[str]]:
        """
        Get just the ticker symbols as a list from Polygon.io

        Args:
            market: Market type (default: "stocks")
            active: Whether to return only active tickers (default: True)

        Returns:
            List of ticker symbols or None if failed
        """
        tickers = self.get_polygon_tickers_all(market, active)
        if tickers:
            return [ticker['ticker'] for ticker in tickers if ticker.get('ticker')]
        return None


