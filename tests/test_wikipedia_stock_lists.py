#!/usr/bin/env python3
"""
Test script for Wikipedia-based stock list fetching
"""
import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.data.stock_lists import get_sp500_symbols

def test_wikipedia_stock_lists():
    """Test the new Wikipedia-based stock list fetchers"""
    try:
        print("=== Testing Wikipedia-Based Stock List Fetchers ===\n")

        # Test S&P 500 symbols
        print("🔍 Testing S&P 500 symbol fetching...")
        sp500_symbols = get_sp500_symbols()

        print(f"\n📊 S&P 500 Results:")
        print(f"  Total symbols: {len(sp500_symbols)}")
        print(f"  Expected range: 400-520 symbols")
        print(f"  Status: {'✅ PASS' if 400 <= len(sp500_symbols) <= 520 else '❌ FAIL'}")

        if sp500_symbols:
            print(f"  First 10 symbols: {sp500_symbols[:10]}")
            print(f"  Last 10 symbols: {sp500_symbols[-10:]}")

            # Check for major S&P 500 stocks
            major_stocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'BRK.B', 'LLY', 'V', 'UNH']
            found_major = [stock for stock in major_stocks if stock in sp500_symbols]
            print(f"  Major stocks found: {found_major}")
            print(f"  Major stocks coverage: {len(found_major)}/{len(major_stocks)}")

        print("\n" + "="*50 + "\n")

        # Summary
        print("📋 SUMMARY:")
        print(f"  S&P 500: {len(sp500_symbols)} symbols {'✅' if 400 <= len(sp500_symbols) <= 520 else '❌'}")

        # Overall test result
        sp500_pass = 400 <= len(sp500_symbols) <= 520

        if sp500_pass:
            print(f"\n🎉 All tests PASSED! Wikipedia scraping is working correctly.")
        else:
            print(f"\n⚠️  Some tests FAILED. Check the output above for details.")

    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_wikipedia_stock_lists()
