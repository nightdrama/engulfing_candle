#!/usr/bin/env python3
"""
Test script for the download_all_events function
"""
import sys
import os
import pandas as pd

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.data.downloader import DataDownloader

def test_download_all_events():
    """Test the download_all_events function with a small sample"""
    try:
        print("Testing download_all_events function...")
        print("=" * 60)

        # Initialize downloader
        downloader = DataDownloader()
        print("✓ DataDownloader initialized successfully")

        # Test with a small limit first to verify functionality
        print("\n1. Testing with limited tickers (first 10 stocks)...")
        print("-" * 40)

        # Get first 10 stock tickers for testing
        all_tickers = downloader.get_polygon_ticker_symbols("stocks", active=True)
        if all_tickers:
            test_tickers = all_tickers[:10]  # Limit to first 10 for testing
            print(f"Testing with first 10 tickers: {test_tickers}")

            # Test the download_multiple_events function that download_all_events uses
            test_events = downloader.download_multiple_events(test_tickers)

            if not test_events.empty:
                print(f"✓ Successfully downloaded events for test tickers")
                print(f"  Total events: {len(test_events)}")
                print(f"  Unique tickers: {test_events['symbol'].nunique()}")

                # Show sample data
                print("\nSample events:")
                sample_events = test_events.head(5)
                for _, event in sample_events.iterrows():
                    print(f"  {event['symbol']}: {event['event_type']} on {event['date']}")
            else:
                print("✗ No events found for test tickers")
        else:
            print("✗ Could not retrieve ticker symbols")
            return

        # Test the actual download_all_events function
        print("\n2. Testing download_all_events function...")
        print("-" * 40)
        print("Note: This will download events for ALL available tickers")
        print("This may take a very long time depending on the number of tickers")

        # Ask user if they want to proceed with full download
        user_input = input("\nDo you want to proceed with downloading ALL events? (y/N): ").strip().lower()

        if user_input in ['y', 'yes']:
            print("\nStarting full download...")
            print("This may take several hours depending on the number of tickers...")

            # Download all events
            all_events = downloader.download_all_events(market="stocks", active=True)

            if not all_events.empty:
                print(f"\n✓ Successfully downloaded ALL corporate events!")
                print(f"  Total events: {len(all_events)}")
                print(f"  Unique tickers: {all_events['symbol'].nunique()}")

                # Show summary statistics
                print("\nEvent type distribution:")
                event_counts = all_events['event_type'].value_counts()
                for event_type, count in event_counts.head(10).items():
                    print(f"  {event_type}: {count}")

                # Show symbol distribution
                print(f"\nTop 10 symbols by event count:")
                symbol_counts = all_events['symbol'].value_counts()
                for symbol, count in symbol_counts.head(10).items():
                    print(f"  {symbol}: {count} events")

                # Check if CSV was saved
                csv_files = [f for f in os.listdir("data/corporate_events") if f.startswith("all_corporate_events_stocks_")]
                if csv_files:
                    latest_csv = max(csv_files, key=lambda x: os.path.getctime(os.path.join("data/corporate_events", x)))
                    print(f"\n✓ CSV file saved: {latest_csv}")
                else:
                    print("\n⚠ No CSV file found in data/corporate_events/")

            else:
                print("✗ No events downloaded")
        else:
            print("Skipping full download test")

        print("\n" + "="*60)
        print("Test completed!")
        print("="*60)

    except Exception as e:
        print(f"✗ Error during testing: {e}")
        import traceback
        traceback.print_exc()

def test_download_all_events_small_market():
    """Test download_all_events with a smaller market for faster testing"""
    try:
        print("\n3. Testing download_all_events with crypto market (smaller)...")
        print("-" * 40)

        downloader = DataDownloader()

        # Test with crypto market which typically has fewer tickers
        crypto_events = downloader.download_all_events(market="crypto", active=True)

        if not crypto_events.empty:
            print(f"✓ Successfully downloaded crypto corporate events!")
            print(f"  Total events: {len(crypto_events)}")
            print(f"  Unique tickers: {crypto_events['symbol'].nunique()}")

            # Show sample data
            print("\nSample crypto events:")
            sample_events = crypto_events.head(5)
            for _, event in sample_events.iterrows():
                print(f"  {event['symbol']}: {event['event_type']} on {event['date']}")
        else:
            print("✗ No crypto events found")

    except Exception as e:
        print(f"✗ Error during crypto market testing: {e}")

if __name__ == "__main__":
    print("Testing download_all_events Function")
    print("=" * 60)

    # Test 1: Basic functionality with limited tickers
    test_download_all_events()

    # Test 2: Small market test (crypto)
    test_download_all_events_small_market()

    print("\nAll tests completed!")

