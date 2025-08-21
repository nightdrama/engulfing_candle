#!/usr/bin/env python3
"""
Test script for the DataDownloader (OHLCV and Earnings data)
"""
import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.data.downloader import DataDownloader
from src.data import get_sp500_symbols

def test_ohlcv_downloader():
    """Test the OHLCV data downloader with sample stocks"""
    try:
        # Initialize downloader
        downloader = DataDownloader()
        print("✓ DataDownloader initialized successfully")

        # Get S&P 500 symbols
        print("Fetching S&P 500 symbols...")
        sp500_symbols = get_sp500_symbols()
        print(f"✓ Retrieved {len(sp500_symbols)} S&P 500 symbols")

        # Test with a small sample (just 2 stocks for testing)
        test_symbols = sp500_symbols[:2]
        print(f"Testing OHLCV download with symbols: {test_symbols}")

        # Download last few trading days
        start_date = "2024-01-01"
        end_date = "2024-12-31"

        print(f"Downloading OHLCV data from {start_date} to {end_date}")

        # Download OHLCV data
        ohlcv_data = downloader.download_multiple_ohlcv(test_symbols, start_date, end_date)

        if ohlcv_data:
            print(f"✓ Successfully downloaded OHLCV data for {len(ohlcv_data)} stocks")

            # Save OHLCV data
            downloader.save_ohlcv_data(ohlcv_data)
            print("✓ OHLCV data saved successfully")

            # Show sample data
            for symbol, df in ohlcv_data.items():
                print(f"\n{symbol} OHLCV data sample:")
                print(df.head())
                print(f"Shape: {df.shape}")
        else:
            print("✗ No OHLCV data downloaded")

    except Exception as e:
        print(f"✗ Error during OHLCV testing: {e}")

def test_corporate_events_downloader():
    """Test the corporate events data downloader with sample stocks"""
    try:
        # Initialize downloader
        downloader = DataDownloader()
        print("✓ DataDownloader initialized successfully")

        # Test with a small sample of well-known stocks
        test_symbols = ['AAPL', 'MSFT', 'GOOGL']
        print(f"Testing corporate events download with symbols: {test_symbols}")

        # Download corporate events data for 2023-2024
        start_date = "2023-01-01"
        end_date = "2024-12-31"

        print(f"Downloading corporate events data from {start_date} to {end_date}")

        # Download corporate events data
        corporate_events_data = downloader.download_multiple_corporate_events(test_symbols)

        if corporate_events_data:
            print(f"✓ Successfully downloaded corporate events data for {len(corporate_events_data)} stocks")

            # Save corporate events data
            csv_file = downloader.save_corporate_events_data(corporate_events_data)
            if csv_file:
                print("✓ Corporate events data saved successfully")

            # Show sample data
            if not corporate_events_data.empty:
                print(f"\nCorporate events summary:")
                print(f"  Total Events: {len(corporate_events_data)}")
                print(f"  Unique Symbols: {corporate_events_data['symbol'].nunique()}")

                # Show sample events
                sample_events = corporate_events_data.head(3)
                for _, event in sample_events.iterrows():
                    print(f"  {event['symbol']}: {event['event_type']} on {event['date']}")
            else:
                print("No corporate events data to display")
        else:
            print("✗ No corporate events data downloaded")

    except Exception as e:
        print(f"✗ Error during corporate events testing: {e}")

def test_combined_functionality():
    """Test both OHLCV and corporate events functionality together"""
    try:
        print("\n" + "="*60)
        print("Testing Combined OHLCV and Corporate Events Functionality")
        print("="*60)

        downloader = DataDownloader()

        # Test symbols
        test_symbols = ['AAPL', 'MSFT']

        # Test date range
        start_date = "2024-01-01"
        end_date = "2024-06-30"

        print(f"Testing combined download for {test_symbols} from {start_date} to {end_date}")

        # Download both data types
        print("\n1. Downloading OHLCV data...")
        ohlcv_data = downloader.download_multiple_ohlcv(test_symbols, start_date, end_date)

        print("\n2. Downloading corporate events data...")
        corporate_events_data = downloader.download_multiple_corporate_events(test_symbols)

        # Save both data types
        if ohlcv_data:
            downloader.save_ohlcv_data(ohlcv_data, "data/temp/")
            print("✓ OHLCV data saved")

        if corporate_events_data:
            downloader.save_corporate_events_data(corporate_events_data, "combined_corporate_events.csv")
            print("✓ Corporate events data saved")

        print(f"\nCombined test completed successfully!")
        print(f"OHLCV stocks: {len(ohlcv_data) if ohlcv_data else 0}")
        print(f"Corporate events stocks: {len(corporate_events_data) if corporate_events_data else 0}")

    except Exception as e:
        print(f"✗ Error during combined testing: {e}")

if __name__ == "__main__":
    print("Testing DataDownloader (OHLCV and Corporate Events)")
    print("=" * 60)

    # Test OHLCV functionality
    print("\n1. Testing OHLCV Downloader")
    print("-" * 30)
    test_ohlcv_downloader()

    # Test corporate events functionality
    print("\n2. Testing Corporate Events Downloader")
    print("-" * 30)
    test_corporate_events_downloader()

    # Test combined functionality
    print("\n3. Testing Combined Functionality")
    print("-" * 30)
    test_combined_functionality()

    print("\n" + "="*60)
    print("Testing completed!")
    print("="*60)
