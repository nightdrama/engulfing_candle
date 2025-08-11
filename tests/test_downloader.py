#!/usr/bin/env python3
"""
Test script for the Polygon data downloader
"""
import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.data.downloader import PolygonDataDownloader
from src.data.stock_lists import get_sp500_symbols

def test_downloader():
    """Test the data downloader with sample stocks"""
    try:
        # Initialize downloader
        downloader = PolygonDataDownloader()
        print("✓ PolygonDataDownloader initialized successfully")

        # Get S&P 500 symbols
        print("Fetching S&P 500 symbols...")
        sp500_symbols = get_sp500_symbols()
        print(f"✓ Retrieved {len(sp500_symbols)} S&P 500 symbols")

        # Test with a small sample (just 2 stocks for testing)
        test_symbols = sp500_symbols
        print(f"Testing with symbols: {test_symbols}")

        # Download last 5 trading days
        start_date = "2020-01-01"  # Recent date
        end_date = "2025-08-08"  # 5 days before

        print(f"Downloading data from {start_date} to {end_date}")

        # Download data
        data = downloader.download_multiple_stocks(test_symbols, start_date, end_date)

        if data:
            print(f"✓ Successfully downloaded data for {len(data)} stocks")

            # Save data
            downloader.save_data(data)
            print("✓ Data saved successfully")

            # Show sample data
            for symbol, df in data.items():
                print(f"\n{symbol} data sample:")
                print(df.head())
                print(f"Shape: {df.shape}")
        else:
            print("✗ No data downloaded")

    except Exception as e:
        print(f"✗ Error during testing: {e}")

if __name__ == "__main__":
    test_downloader()
