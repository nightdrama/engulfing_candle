"""
Engulfing pattern detection implementation
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from .base_pattern import BasePattern

class EngulfingPattern(BasePattern):
    """Detects bullish and bearish engulfing candlestick patterns"""

    def __init__(self):
        super().__init__(
            name="Engulfing Pattern",
            description="Detects bullish and bearish engulfing candlestick patterns"
        )

    def detect(self, df: pd.DataFrame) -> pd.Series:
        """
        Detect engulfing patterns in the given DataFrame

        Args:
            df: DataFrame with columns: date, open, high, low, close, volume

        Returns:
            pd.Series with values: 1 (bullish engulfing), -1 (bearish engulfing), 0 (no pattern)
        """
        if not self.validate_data(df):
            return pd.Series([0] * len(df), index=df.index)

        # Initialize result series
        patterns = pd.Series([0] * len(df), index=df.index)

        # Need at least 2 candles for engulfing pattern
        if len(df) < 2:
            return patterns

        # Get OHLC data
        opens = df['open'].values
        highs = df['high'].values
        lows = df['low'].values
        closes = df['close'].values

        # Detect patterns starting from second candle (index 1)
        for i in range(1, len(df)):
            prev_open = opens[i-1]
            prev_close = closes[i-1]
            curr_open = opens[i]
            curr_close = closes[i]

            # Bullish engulfing: current candle completely engulfs previous bearish candle
            if (prev_close < prev_open and  # Previous candle is bearish
                curr_close > curr_open and   # Current candle is bullish
                curr_open < prev_close and   # Current open below previous close
                curr_close > prev_open):     # Current close above previous open

                patterns.iloc[i] = 1  # Bullish engulfing

            # Bearish engulfing: current candle completely engulfs previous bullish candle
            elif (prev_close > prev_open and  # Previous candle is bullish
                  curr_close < curr_open and   # Current candle is bearish
                  curr_open > prev_close and   # Current open above previous close
                  curr_close < prev_open):     # Current close below previous open

                patterns.iloc[i] = -1  # Bearish engulfing

        return patterns

    def get_signal(self, df: pd.DataFrame) -> pd.Series:
        """
        Get trading signals based on engulfing pattern detection

        Args:
            df: DataFrame with OHLCV data

        Returns:
            pd.Series with signal values: 1 (buy), -1 (sell), 0 (no signal)
        """
        patterns = self.detect(df)

        # Convert pattern detection to trading signals
        signals = pd.Series([0] * len(df), index=df.index)

        # Bullish engulfing = buy signal
        signals[patterns == 1] = 1

        # Bearish engulfing = sell signal
        signals[patterns == -1] = -1

        return signals

    def get_pattern_details(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """
        Get detailed pattern information

        Args:
            df: DataFrame with OHLCV data

        Returns:
            Dictionary with pattern details
        """
        patterns = self.detect(df)

        return {
            'patterns': patterns,
            'signals': self.get_signal(df),
            'bullish_count': (patterns == 1).sum(),
            'bearish_count': (patterns == -1).sum(),
            'total_patterns': (patterns != 0).sum()
        }
