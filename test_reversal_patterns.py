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
        stock_data = read_polygon_data(data_path, start_date=start_date, end_date=end_date)

        if not stock_data:
            print("No data found for this period")
            return

        print(f"Loaded data for {len(stock_data)} stocks")

        # Prepare data for CSV output
        pattern_records = []

        # Analyze stocks (limit to first 100 for performance)
        analyzed_stocks = 0
        max_stocks = 100

        for symbol, df in stock_data.items():
            if analyzed_stocks >= max_stocks:
                break

            if len(df) < 10:  # Skip stocks with insufficient data for return calculation
                continue

            print(f"Analyzing {symbol} ({len(df)} records)...")

            # Detect reversal patterns
            reversal_patterns = reversal_detector.detect(df)
            reversal_breakdown = reversal_detector.get_pattern_breakdown(df)

            # Detect engulfing patterns
            engulfing_patterns = engulfing_detector.detect(df)

                        # Get pattern details for each occurrence
            # Process reversal patterns
            for i, (date, pattern) in enumerate(zip(df['date'], reversal_patterns)):
                if pattern != 0:
                    # Determine specific pattern type
                    pattern_name = get_specific_pattern_name(reversal_detector, df, i, reversal_breakdown)

                    # Get current price
                    current_price = df.iloc[i]['close']

                    # Calculate forward returns
                    forward_1d = calculate_forward_return(df, i, 1)
                    forward_5d = calculate_forward_return(df, i, 5)
                    forward_10d = calculate_forward_return(df, i, 10)
                    forward_20d = calculate_forward_return(df, i, 20)

                    # Calculate backward returns
                    backward_1d = calculate_backward_return(df, i, 1)
                    backward_5d = calculate_backward_return(df, i, 5)

                    # Create record
                    record = {
                        'symbol': symbol,
                        'date': date.strftime('%Y-%m-%d'),
                        'pattern_name': pattern_name,
                        'pattern_direction': 'Bullish' if pattern == 1 else 'Bearish',
                        'price_at_pattern': current_price,
                        'forward_1d_return': forward_1d,
                        'forward_5d_return': forward_5d,
                        'forward_10d_return': forward_10d,
                        'forward_20d_return': forward_20d,
                        'backward_1d_return': backward_1d,
                        'backward_5d_return': backward_5d
                    }

                    pattern_records.append(record)

            # Process engulfing patterns
            for i, (date, pattern) in enumerate(zip(df['date'], engulfing_patterns)):
                if pattern != 0:
                    # Determine engulfing pattern type
                    pattern_name = 'bullish_engulfing' if pattern == 1 else 'bearish_engulfing'

                    # Get current price
                    current_price = df.iloc[i]['close']

                    # Calculate forward returns
                    forward_1d = calculate_forward_return(df, i, 1)
                    forward_5d = calculate_forward_return(df, i, 5)
                    forward_10d = calculate_forward_return(df, i, 10)
                    forward_20d = calculate_forward_return(df, i, 20)

                    # Calculate backward returns
                    backward_1d = calculate_backward_return(df, i, 1)
                    backward_5d = calculate_backward_return(df, i, 5)

                    # Create record
                    record = {
                        'symbol': symbol,
                        'date': date.strftime('%Y-%m-%d'),
                        'pattern_name': pattern_name,
                        'pattern_direction': 'Bullish' if pattern == 1 else 'Bearish',
                        'price_at_pattern': current_price,
                        'forward_1d_return': forward_1d,
                        'forward_5d_return': forward_5d,
                        'forward_10d_return': forward_10d,
                        'forward_20d_return': forward_20d,
                        'backward_1d_return': backward_1d,
                        'backward_5d_return': backward_5d
                    }

                    pattern_records.append(record)

            analyzed_stocks += 1

        # Create DataFrame and save to CSV
        if pattern_records:
            results_df = pd.DataFrame(pattern_records)

            # Save to CSV
            output_file = 'reversal_pattern_analysis.csv'
            results_df.to_csv(output_file, index=False)

            print(f"\n" + "=" * 60)
            print("Analysis Complete!")
            print("=" * 60)
            print(f"Total pattern occurrences: {len(pattern_records)}")
            print(f"Stocks analyzed: {analyzed_stocks}")
            print(f"Results saved to: {output_file}")

            # Show summary statistics
            print(f"\nPattern Summary:")
            pattern_counts = results_df['pattern_name'].value_counts()
            for pattern, count in pattern_counts.items():
                print(f"  {pattern}: {count}")

            print(f"\nReturn Statistics:")
            return_columns = ['forward_1d_return', 'forward_5d_return', 'forward_10d_return',
                            'forward_20d_return', 'backward_1d_return', 'backward_5d_return']

            for col in return_columns:
                valid_returns = results_df[col].dropna()
                if len(valid_returns) > 0:
                    mean_return = valid_returns.mean()
                    print(f"  {col}: {mean_return:.4f} ({len(valid_returns)} valid observations)")

            # Show sample of results
            print(f"\nSample Results:")
            print(results_df.head(10).to_string(index=False))

        else:
            print("No patterns detected in the analyzed stocks")

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
