"""
Polygon.io data downloader for daily OHLCV data
"""
import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class PolygonDataDownloader:
    """Downloads daily OHLCV data from Polygon.io API"""

    def __init__(self):
        self.api_key = os.getenv('POLYGON_API_KEY')
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY environment variable not set")

        self.base_url = "https://api.polygon.io"
        self.rate_limit_delay = 0.1  # 100ms between requests

    def download_daily_data(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        Download daily OHLCV data for a single stock

        Args:
            symbol: Stock ticker symbol
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            DataFrame with columns: date, open, high, low, close, volume
        """
        try:
            url = f"{self.base_url}/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
            params = {
                'apiKey': self.api_key,
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

            # Rate limiting
            time.sleep(self.rate_limit_delay)

            return df

        except requests.exceptions.RequestException as e:
            print(f"Error downloading data for {symbol}: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error for {symbol}: {e}")
            return None

    def download_multiple_stocks(self, symbols: List[str], start_date: str, end_date: str) -> dict:
        """
        Download data for multiple stocks

        Args:
            symbols: List of stock tickers
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            Dictionary with symbol as key and DataFrame as value
        """
        results = {}

        for symbol in symbols:
            print(f"Downloading data for {symbol}...")
            data = self.download_daily_data(symbol, start_date, end_date)
            if data is not None:
                results[symbol] = data
                print(f"Successfully downloaded {len(data)} days of data for {symbol}")
            else:
                print(f"Failed to download data for {symbol}")

        return results

    def save_data(self, data_dict: dict, output_dir: str = "data/raw"):
        """
        Save downloaded data to CSV files

        Args:
            data_dict: Dictionary of DataFrames from download_multiple_stocks
            output_dir: Directory to save CSV files
        """
        os.makedirs(output_dir, exist_ok=True)

        for symbol, df in data_dict.items():
            filename = f"{output_dir}/{symbol}_daily.csv"
            df.to_csv(filename, index=False)
            print(f"Saved {symbol} data to {filename}")
