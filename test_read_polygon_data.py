#!/usr/bin/env python3
"""
Simplified test script to read stock OHLCV data from Polygon data folder
"""
import os
import pandas as pd
import gzip
from datetime import datetime, timedelta
import glob

def read_polygon_data(data_path: str, start_date: str = None, end_date: str = None):
    """
    Read stock OHLCV data from Polygon folder structure

    Args:
        data_path: Path to day_aggs_v1 folder
        start_date: Optional start date in YYYY-MM-DD format (if None, reads all files)
        end_date: Optional end date in YYYY-MM-DD format (if None, reads all files)

    Returns:
        DataFrame with columns: symbol, date, open, high, low, close, volume
    """
    all_records = []

    # Parse date filters if provided
    start_dt = None
    end_dt = None
    if start_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    if end_date:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # If date filters are provided, only get files for those dates
    if start_dt and end_dt:
        # Generate list of dates in range
        current_date = start_dt
        target_files = []

        while current_date <= end_dt:
            # Construct file path based on date
            year = current_date.year
            month = current_date.month
            day = current_date.day

            # Format date for filename
            date_str = current_date.strftime("%Y-%m-%d")
            filename = f"{date_str}.csv.gz"

            # Construct full path
            file_path = os.path.join(data_path, str(year), f"{month:02d}", filename)

            if os.path.exists(file_path):
                target_files.append(file_path)

            # Move to next day
            current_date += timedelta(days=1)

        csv_files = target_files
        print(f"Date range specified: {start_date} to {end_date}")
        print(f"Looking for {len(csv_files)} specific files")

    else:
        # Get all CSV files if no date filter
        pattern = os.path.join(data_path, "**/*.csv.gz")
        csv_files = glob.glob(pattern, recursive=True)
        print(f"No date filter - found {len(csv_files)} total CSV files")

    processed_files = 0
    for file_path in csv_files:
        # Extract date from filename
        filename = os.path.basename(file_path)
        if not filename.endswith('.csv.gz'):
            continue

        date_str = filename.replace('.csv.gz', '')
        try:
            file_date = datetime.strptime(date_str, "%Y-%m-%d")

            # Additional date filter check (in case of edge cases)
            if start_dt and file_date < start_dt:
                continue
            if end_dt and file_date > end_dt:
                continue

            print(f"Processing {filename}...")

            # Read compressed CSV
            with gzip.open(file_path, 'rt') as f:
                df = pd.read_csv(f)

            # Process each stock in the file
            for _, row in df.iterrows():
                symbol = row['ticker']  # Column is named 'ticker' not 'symbol'

                # Create stock record
                record = {
                    'symbol': symbol,
                    'date': file_date,
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'volume': row['volume']
                }

                all_records.append(record)

            processed_files += 1

        except Exception as e:
            print(f"Error processing {filename}: {e}")
            continue

    print(f"Successfully processed {processed_files} files")

    # Create single DataFrame
    if all_records:
        result_df = pd.DataFrame(all_records)
        result_df = result_df.sort_values(['symbol', 'date'])
        print(f"Total records: {len(result_df)}")
        print(f"Unique symbols: {result_df['symbol'].nunique()}")
        return result_df
    else:
        return pd.DataFrame()

def main():
    """Main function to test data reading"""
    data_path = "../Data/polygon/us_stocks_sip/day_aggs_v1"

    print("Reading all Polygon stock data...")
    stock_df = read_polygon_data(data_path, start_date='2025-08-01', end_date='2025-08-20')

    if stock_df.empty:
        print("No data loaded")
        return

    print(f"\nLoaded data for {stock_df['symbol'].nunique()} stocks")
    print(f"Total records: {len(stock_df)}")

    # Show sample data for first 3 symbols
    unique_symbols = stock_df['symbol'].unique()[:3]
    for symbol in unique_symbols:
        symbol_data = stock_df[stock_df['symbol'] == symbol]
        print(f"\n{symbol} - {len(symbol_data)} records:")
        print(symbol_data.head())
        print(f"Date range: {symbol_data['date'].min()} to {symbol_data['date'].max()}")

if __name__ == "__main__":
    main()
