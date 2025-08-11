"""
Stock universe definitions using Wikipedia
"""
import requests
import pandas as pd
from typing import List, Optional
import time

class StockUniverseDownloader:
    """Downloads stock universe lists from Wikipedia"""

    def __init__(self):
        self.rate_limit_delay = 0.5  # 500ms between requests to be respectful

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

# Convenience functions
def get_sp500_symbols() -> List[str]:
    """Get S&P 500 symbols"""
    downloader = StockUniverseDownloader()
    return downloader.get_sp500_symbols()

# Legacy names for backward compatibility
SP500_SAMPLE = get_sp500_symbols
