#!/usr/bin/env python3
"""
Test script for reversal pattern detection using Polygon data
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.strategies.reversal_patterns import ReversalPatterns
from src.strategies.engulfing import EngulfingPattern
from test_read_polygon_data import read_polygon_data

def test_reversal_patterns_with_polygon_data():
    """Test reversal pattern detection with real Polygon data and generate comprehensive CSV"""
    print("Testing Reversal Pattern Detection with Polygon Data")
    print("=" * 60)

        # Initialize pattern detectors
    reversal_detector = ReversalPatterns()
    engulfing_detector = EngulfingPattern()

    # Read Polygon data
    data_path = "../Data/polygon/us_stocks_sip/day_aggs_v1"
    print("Reading Polygon data...")

        # Use a longer period to get more data for analysis
    start_date = "2025-06-01"
    end_date = "2025-07-31"

    try:
        stock_df = read_polygon_data(data_path, start_date=start_date, end_date=end_date)

        if stock_df.empty:
            print("No data found for this period")
            return

        print(f"Loaded data for {stock_df['symbol'].nunique()} stocks")
        print(f"Total records: {len(stock_df)}")

        # Prepare data for CSV output
        pattern_records = []

        # Analyze stocks (limit to first 100 for performance)
        analyzed_stocks = 0
        max_stocks = 100

        # Get unique symbols
        unique_symbols = stock_df['symbol'].unique()

        for symbol in unique_symbols:
            if analyzed_stocks >= max_stocks:
                break

            # Get data for this symbol
            df = stock_df[stock_df['symbol'] == symbol].copy()
            df = df.sort_values('date').reset_index(drop=True)

            if len(df) < 10:  # Skip stocks with insufficient data for return calculation
                continue

            print(f"Analyzing {symbol} ({len(df)} records)...")

            # Detect reversal patterns
            reversal_patterns = reversal_detector.detect(df)
            reversal_breakdown = reversal_detector.get_pattern_breakdown(df)

            # Detect engulfing patterns
            engulfing_patterns = engulfing_detector.detect(df)

                        # Get pattern details for each occurrence
            # Process both pattern types
            for pattern_type, patterns in [('reversal', reversal_patterns), ('engulfing', engulfing_patterns)]:
                for i, (date, pattern) in enumerate(zip(df['date'], patterns)):
                    if pattern != 0:
                        # Get pattern name
                        if pattern_type == 'reversal':
                            pattern_name = get_specific_pattern_name(reversal_detector, df, i, reversal_breakdown)
                        else:
                            pattern_name = 'bullish_engulfing' if pattern == 1 else 'bearish_engulfing'

                        # Calculate returns
                        returns = {
                            'forward_1d_return': calculate_forward_return(df, i, 1),
                            'forward_5d_return': calculate_forward_return(df, i, 5),
                            'forward_10d_return': calculate_forward_return(df, i, 10),
                            'forward_20d_return': calculate_forward_return(df, i, 20),
                            'backward_1d_return': calculate_backward_return(df, i, 1),
                            'backward_5d_return': calculate_backward_return(df, i, 5)
                        }

                        # Create record
                        record = {
                            'symbol': symbol,
                            'date': date.strftime('%Y-%m-%d'),
                            'pattern_name': pattern_name,
                            'pattern_direction': 'Bullish' if pattern == 1 else 'Bearish',
                            'price_at_pattern': df.iloc[i]['close'],
                            **returns
                        }

                        pattern_records.append(record)

            analyzed_stocks += 1

        # Process and save results
        if pattern_records:
            results_df = pd.DataFrame(pattern_records)
            output_file = 'reversal_pattern_analysis.csv'
            results_df.to_csv(output_file, index=False)

            # Print summary
            print(f"\n{'='*60}")
            print("Analysis Complete!")
            print(f"{'='*60}")
            print(f"Total patterns: {len(pattern_records)} | Stocks: {analyzed_stocks} | File: {output_file}")

            # Pattern counts
            print(f"\nPattern Summary:\n{results_df['pattern_name'].value_counts().to_string()}")

            # Return statistics
            return_cols = ['forward_1d_return', 'forward_5d_return', 'forward_10d_return',
                          'forward_20d_return', 'backward_1d_return', 'backward_5d_return']
            print(f"\nReturn Statistics:")
            for col in return_cols:
                valid = results_df[col].dropna()
                if len(valid) > 0:
                    print(f"  {col}: {valid.mean():.4f} ({len(valid)} obs)")

            print(f"\nSample Results:\n{results_df.head(10).to_string(index=False)}")
        else:
            print("No patterns detected")

    except Exception as e:
        print(f"Error in analysis: {e}")
        import traceback
        traceback.print_exc()

def get_specific_pattern_name(detector, df, index, breakdown):
    """Get the specific name of the pattern at the given index"""
    opens = df['open'].values
    highs = df['high'].values
    lows = df['low'].values
    closes = df['close'].values

    # Check each pattern type
    if detector._is_hammer(opens[index], highs[index], lows[index], closes[index]):
        return 'hammer'
    elif detector._is_shooting_star(opens[index], highs[index], lows[index], closes[index]):
        return 'shooting_star'
    elif detector._is_doji(opens[index], highs[index], lows[index], closes[index]):
        return 'doji'
    elif index >= 2 and detector._is_morning_star(df, index):
        return 'morning_star'
    elif index >= 2 and detector._is_evening_star(df, index):
        return 'evening_star'
    else:
        return 'unknown'

def calculate_forward_return(df, index, days):
    """Calculate forward return for specified number of days"""
    if index + days >= len(df):
        return None  # Not enough forward data

    current_price = df.iloc[index]['close']
    future_price = df.iloc[index + days]['close']

    return (future_price - current_price) / current_price

def calculate_backward_return(df, index, days):
    """Calculate backward return for specified number of days"""
    if index - days < 0:
        return None  # Not enough backward data

    current_price = df.iloc[index]['close']
    past_price = df.iloc[index - days]['close']

    return (current_price - past_price) / past_price

def test_pattern_validation():
    """Test pattern validation with edge cases"""
    print(f"\n" + "=" * 60)
    print("Pattern Validation Tests")
    print("=" * 60)

    detector = ReversalPatterns()

    # Test with invalid data
    print("Testing with invalid data...")

    # Empty DataFrame
    empty_df = pd.DataFrame()
    patterns = detector.detect(empty_df)
    print(f"Empty DataFrame: {len(patterns)} patterns detected")

    # DataFrame with missing columns
    invalid_df = pd.DataFrame({'date': [1, 2, 3], 'price': [100, 101, 102]})
    patterns = detector.detect(invalid_df)
    print(f"Invalid columns: {len(patterns)} patterns detected")

    # DataFrame with null values
    null_df = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=5),
        'open': [100, 101, np.nan, 103, 104],
        'high': [102, 103, 104, 105, 106],
        'low': [99, 100, 101, 102, 103],
        'close': [101, 102, 103, 104, 105],
        'volume': [1000] * 5
    })
    patterns = detector.detect(null_df)
    print(f"Null values: {len(patterns)} patterns detected")

if __name__ == "__main__":
    test_reversal_patterns_with_polygon_data()
    test_pattern_validation()
