# Project Plan
## Goal
Create a trading strategy backtester that analyze the investment performance of the bullish and bearish engulfing patterns from daily OHLCV charts.

## Steps
1. Create a module download daily OHLCV data from Polygon.io and I have provided API key as one of the environment variables. Use Polygon.io REST API. Test it with downloading a few days of daily of S&P 500 and Russell 1000 stocks.

2. Create a simple pattern detection module that recognize the bullish and bearish engulfing patterns. The module functions and folder structure should be generic enough such that we could add other patterns in the future.

3. Create a simple backtester examing the performance of long bullish engulfing patterns and short bearish engulfing patterns. Test it on S&P500 stocks for 2025 YTD. Assume some simple and reasonble stop wins and stop losses. Assume reasonable transaction costs. Showing simple statistics such as hit rate, average returns, and list the detailed trade for all stocks.

4. Create a module download earning date data from yahoo finance. For now, just download the earning date data without any extra information.

5. Re-test the investment performance of bullish and bearish engulfing patterns when stocks report earnings in the past week (within the T-5 to T-1 window).

## Constraints
- Keep code minimal and manageable
- Keep edits ≤ 100 lines per step
- Touch one file per step unless approved

