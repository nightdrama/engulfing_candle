#!/usr/bin/env python3
"""
Test script for the DataDownloader (OHLCV and Earnings data)
"""
import sys
import os
from src.data.downloader import DataDownloader

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


if __name__ == "__main__":

    downloader = DataDownloader()
    downloader.download_all_events()
