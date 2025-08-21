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
        Dict mapping stock symbols to DataFrames
    """
    stock_data = {}

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
                    'date': file_date,
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'volume': row['volume']
                }

                if symbol not in stock_data:
                    stock_data[symbol] = []
                stock_data[symbol].append(record)

            processed_files += 1

        except Exception as e:
            print(f"Error processing {filename}: {e}")
            continue

    print(f"Successfully processed {processed_files} files")

    # Convert lists to DataFrames
    for symbol in stock_data:
        stock_data[symbol] = pd.DataFrame(stock_data[symbol])
        stock_data[symbol] = stock_data[symbol].sort_values('date')

    return stock_data

def main():
    """Main function to test data reading"""
    data_path = "../Data/polygon/us_stocks_sip/day_aggs_v1"

    print("Reading all Polygon stock data...")
    stock_data = read_polygon_data(data_path,start_date = '2025-08-01', end_date = '2025-08-20')  # No date filters - reads all files

    print(f"\nLoaded data for {len(stock_data)} stocks")

    # Show sample data
    for symbol in list(stock_data.keys())[:3]:
        df = stock_data[symbol]
        print(f"\n{symbol} - {len(df)} records:")
        print(df.head())
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")

if __name__ == "__main__":
    main()
