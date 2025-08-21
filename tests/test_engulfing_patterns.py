#!/usr/bin/env python3
"""
Test script for engulfing pattern detection on S&P 500 stocks
"""
import sys
import os
import pandas as pd
from datetime import datetime, timedelta

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.data.downloader import PolygonDataDownloader
from src.data import get_sp500_symbols
from src.strategies.engulfing import EngulfingPattern

def test_engulfing_patterns():
    """Test engulfing pattern detection on S&P 500 stocks"""
    try:
        print("=== Engulfing Pattern Detection Test ===")

        # Initialize components
        downloader = PolygonDataDownloader()
        pattern_detector = EngulfingPattern()

        print("✓ Components initialized successfully")

        # Get S&P 500 symbols
        print("Fetching S&P 500 symbols...")
        sp500_symbols = get_sp500_symbols()
        print(f"✓ Retrieved {len(sp500_symbols)} S&P 500 symbols")

                # Calculate date range (July 1 to August 8, 2025)
        start_date = "2025-07-01"
        end_date = "2025-08-08"

        print(f"Analyzing data from {start_date} to {end_date}")

        # Initialize results storage
        bullish_results = []
        bearish_results = []

        # Test with all S&P 500 stocks
        test_symbols = sp500_symbols
        print(f"Testing with all {len(test_symbols)} S&P 500 stocks")

        for i, symbol in enumerate(test_symbols, 1):
            print(f"\n[{i}/{len(test_symbols)}] Processing {symbol}...")

            try:
                # Download data for this stock
                data = downloader.download_ohlcv_data(symbol, start_date, end_date)

                if data is None or len(data) < 2:
                    print(f"  ⚠️  Insufficient data for {symbol}")
                    continue

                # Detect patterns
                pattern_details = pattern_detector.get_pattern_details(data)

                # Process bullish engulfing patterns
                bullish_patterns = pattern_details['patterns'][pattern_details['patterns'] == 1]
                for date, pattern in bullish_patterns.items():
                    # Get the actual date from the data DataFrame
                    actual_date = data.loc[date, 'date']
                    # Format date as YYYY-MM-DD string
                    formatted_date = actual_date.strftime('%Y-%m-%d') if hasattr(actual_date, 'strftime') else str(actual_date)

                    bullish_results.append({
                        'symbol': symbol,
                        'date': formatted_date,
                        'pattern_type': 'bullish_engulfing',
                        'open': data.loc[date, 'open'],
                        'high': data.loc[date, 'open'],
                        'low': data.loc[date, 'low'],
                        'close': data.loc[date, 'close'],
                        'volume': data.loc[date, 'volume']
                    })

                # Process bearish engulfing patterns
                bearish_patterns = pattern_details['patterns'][pattern_details['patterns'] == -1]
                for date, pattern in bearish_patterns.items():
                    # Get the actual date from the data DataFrame
                    actual_date = data.loc[date, 'date']
                    # Format date as YYYY-MM-DD string
                    formatted_date = actual_date.strftime('%Y-%m-%d') if hasattr(actual_date, 'strftime') else str(actual_date)

                    bearish_results.append({
                        'symbol': symbol,
                        'date': formatted_date,
                        'pattern_type': 'bearish_engulfing',
                        'open': data.loc[date, 'open'],
                        'high': data.loc[date, 'high'],
                        'low': data.loc[date, 'low'],
                        'close': data.loc[date, 'close'],
                        'volume': data.loc[date, 'volume']
                    })

                print(f"  ✓ Found {len(bullish_patterns)} bullish, {len(bearish_patterns)} bearish patterns")

            except Exception as e:
                print(f"  ✗ Error processing {symbol}: {e}")
                continue

        # Save results to CSV files
        if bullish_results:
            bullish_df = pd.DataFrame(bullish_results)
            bullish_file = "data/results/bullish_engulfing_patterns.csv"
            bullish_df.to_csv(bullish_file, index=False)
            print(f"\n✓ Saved {len(bullish_results)} bullish patterns to {bullish_file}")
        else:
            print("\n⚠️  No bullish engulfing patterns found")

        if bearish_results:
            bearish_df = pd.DataFrame(bearish_results)
            bearish_file = "data/results/bearish_engulfing_patterns.csv"
            bearish_df.to_csv(bearish_file, index=False)
            print(f"✓ Saved {len(bearish_results)} bearish patterns to {bearish_file}")
        else:
            print("⚠️  No bearish engulfing patterns found")

        # Summary
        total_patterns = len(bullish_results) + len(bearish_results)
        print(f"\n=== Summary ===")
        print(f"Total stocks processed: {len(test_symbols)}")
        print(f"Total patterns found: {total_patterns}")
        print(f"Bullish patterns: {len(bullish_results)}")
        print(f"Bearish patterns: {len(bearish_results)}")

        if total_patterns > 0:
            print(f"\nPattern detection test PASSED ✓")
        else:
            print(f"\nPattern detection test - No patterns found (may need more data)")

    except Exception as e:
        print(f"✗ Error during testing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_engulfing_patterns()
