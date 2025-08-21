"""
Reversal pattern detection implementation
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from .base_pattern import BasePattern

class ReversalPatterns(BasePattern):
    """Detects multiple bullish and bearish reversal candlestick patterns"""

    def __init__(self):
        super().__init__(
            name="Reversal Patterns",
            description="Detects multiple bullish and bearish reversal candlestick patterns including hammer, shooting star, doji, and morning/evening star"
        )

    def detect(self, df: pd.DataFrame) -> pd.Series:
        """
        Detect reversal patterns in the given DataFrame

        Args:
            df: DataFrame with columns: date, open, high, low, close, volume

        Returns:
            pd.Series with values: 1 (bullish reversal), -1 (bearish reversal), 0 (no pattern)
        """
        if not self.validate_data(df):
            return pd.Series([0] * len(df), index=df.index)

        # Initialize result series
        patterns = pd.Series([0] * len(df), index=df.index)

        # Need at least 3 candles for some patterns
        if len(df) < 3:
            return patterns

        # Get OHLC data
        opens = df['open'].values
        highs = df['high'].values
        lows = df['low'].values
        closes = df['close'].values

        # Detect patterns starting from second candle (index 1)
        for i in range(1, len(df)):
            # Single candle patterns
            prev_close = closes[i-1] if i > 0 else None
            if self._is_hammer(opens[i], highs[i], lows[i], closes[i], prev_close):
                patterns.iloc[i] = 1  # Bullish reversal
            elif self._is_shooting_star(opens[i], highs[i], lows[i], closes[i]):
                patterns.iloc[i] = -1  # Bearish reversal
            elif self._is_doji(opens[i], highs[i], lows[i], closes[i]):
                # Doji is neutral, but can be reversal in context
                patterns.iloc[i] = self._analyze_doji_context(df, i)

            # Multi-candle patterns (need at least 3 candles)
            if i >= 2:
                if self._is_morning_star(df, i):
                    patterns.iloc[i] = 1  # Bullish reversal
                elif self._is_evening_star(df, i):
                    patterns.iloc[i] = -1  # Bearish reversal

        return patterns

    def _is_hammer(self, open_price: float, high: float, low: float, close: float, prev_close: float = None) -> bool:
        """Detect hammer pattern (bullish reversal)"""
        body = abs(close - open_price)
        lower_shadow = min(open_price, close) - low
        upper_shadow = high - max(open_price, close)

        # Hammer criteria:
        # 1. Small body (less than 30% of total range)
        # 2. Long lower shadow (at least 2x body)
        # 3. Small or no upper shadow
        # 4. Opens below previous close (if prev_close provided)
        # 5. Close above today's open
        total_range = high - low
        if total_range == 0 or close < open_price:
            return False

        body_ratio = body / total_range
        shadow_ratio = lower_shadow / body if body > 0 else 0
        upper_ratio = upper_shadow / total_range

        # Basic hammer criteria
        hammer_criteria = (body_ratio < 0.3 and
                          shadow_ratio >= 2.0 and
                          upper_ratio < 0.1)

        # Additional condition: opens below previous close
        if prev_close is not None:
            hammer_criteria = hammer_criteria and (open_price < prev_close)

        return hammer_criteria

    def _is_shooting_star(self, open_price: float, high: float, low: float, close: float) -> bool:
        """Detect shooting star pattern (bearish reversal)"""
        body = abs(close - open_price)
        lower_shadow = min(open_price, close) - low
        upper_shadow = high - max(open_price, close)

        # Shooting star criteria:
        # 1. Small body (less than 30% of total range)
        # 2. Long upper shadow (at least 2x body)
        # 3. Small or no lower shadow
        total_range = high - low
        if total_range == 0:
            return False

        body_ratio = body / total_range
        shadow_ratio = upper_shadow / body if body > 0 else 0
        lower_ratio = lower_shadow / total_range

        return (body_ratio < 0.3 and
                shadow_ratio >= 2.0 and
                lower_ratio < 0.1)

    def _is_doji(self, open_price: float, high: float, low: float, close: float) -> bool:
        """Detect doji pattern (neutral, potential reversal)"""
        body = abs(close - open_price)
        total_range = high - low

        if total_range == 0:
            return False

        # Doji criteria: very small body (less than 10% of total range)
        body_ratio = body / total_range
        return body_ratio < 0.1

    def _analyze_doji_context(self, df: pd.DataFrame, index: int) -> int:
        """Analyze doji in context to determine reversal direction"""
        if index == 0 or index >= len(df) - 1:
            return 0

        prev_close = df.iloc[index-1]['close']
        curr_close = df.iloc[index]['close']
        next_close = df.iloc[index+1]['close']

        # If doji appears after downtrend and next candle is bullish
        if prev_close > curr_close and next_close > curr_close:
            return 1  # Bullish reversal
        # If doji appears after uptrend and next candle is bearish
        elif prev_close < curr_close and next_close < curr_close:
            return -1  # Bearish reversal

        return 0  # Neutral

    def _is_morning_star(self, df: pd.DataFrame, index: int) -> bool:
        """Detect morning star pattern (bullish reversal)"""
        if index < 2:
            return False

        # Get the three candles
        first_open = df.iloc[index-2]['open']
        first_close = df.iloc[index-2]['close']
        second_open = df.iloc[index-1]['open']
        second_close = df.iloc[index-1]['close']
        third_open = df.iloc[index]['open']
        third_close = df.iloc[index]['close']

        # Morning star criteria:
        # 1. First candle: bearish (close < open)
        # 2. Second candle: small body, gaps down from first
        # 3. Third candle: bullish, closes above midpoint of first candle
        first_body = abs(first_close - first_open)
        second_body = abs(second_close - second_open)
        third_body = abs(third_close - third_open)

        first_midpoint = (first_open + first_close) / 2

        return (first_close < first_open and  # First candle bearish
                second_body < first_body * 0.3 and  # Second candle small
                second_close < first_close and  # Second candle gaps down
                third_close > third_open and  # Third candle bullish
                third_close > first_midpoint)  # Third candle closes above midpoint

    def _is_evening_star(self, df: pd.DataFrame, index: int) -> bool:
        """Detect evening star pattern (bearish reversal)"""
        if index < 2:
            return False

        # Get the three candles
        first_open = df.iloc[index-2]['open']
        first_close = df.iloc[index-2]['close']
        second_open = df.iloc[index-1]['open']
        second_close = df.iloc[index-1]['close']
        third_open = df.iloc[index]['open']
        third_close = df.iloc[index]['close']

        # Evening star criteria:
        # 1. First candle: bullish (close > open)
        # 2. Second candle: small body, gaps up from first
        # 3. Third candle: bearish, closes below midpoint of first candle
        first_body = abs(first_close - first_open)
        second_body = abs(second_close - second_open)
        third_body = abs(third_close - third_open)

        first_midpoint = (first_open + first_close) / 2

        return (first_close > first_open and  # First candle bullish
                second_body < first_body * 0.3 and  # Second candle small
                second_close > first_close and  # Second candle gaps up
                third_close < third_open and  # Third candle bearish
                third_close < first_midpoint)  # Third candle closes below midpoint

    def get_signal(self, df: pd.DataFrame) -> pd.Series:
        """
        Get trading signals based on reversal pattern detection

        Args:
            df: DataFrame with OHLCV data

        Returns:
            pd.Series with signal values: 1 (buy), -1 (sell), 0 (no signal)
        """
        patterns = self.detect(df)

        # Convert pattern detection to trading signals
        signals = pd.Series([0] * len(df), index=df.index)

        # Bullish reversal patterns = buy signal
        signals[patterns == 1] = 1

        # Bearish reversal patterns = sell signal
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

    def get_pattern_breakdown(self, df: pd.DataFrame) -> Dict[str, int]:
        """
        Get breakdown of specific pattern types detected

        Args:
            df: DataFrame with OHLCV data

        Returns:
            Dictionary with counts of each pattern type
        """
        if not self.validate_data(df):
            return {}

        opens = df['open'].values
        highs = df['high'].values
        lows = df['low'].values
        closes = df['close'].values

        pattern_counts = {
            'hammer': 0,
            'shooting_star': 0,
            'doji': 0,
            'morning_star': 0,
            'evening_star': 0
        }

        for i in range(1, len(df)):
            prev_close = closes[i-1] if i > 0 else None
            if self._is_hammer(opens[i], highs[i], lows[i], closes[i], prev_close):
                pattern_counts['hammer'] += 1
            elif self._is_shooting_star(opens[i], highs[i], lows[i], closes[i]):
                pattern_counts['shooting_star'] += 1
            elif self._is_doji(opens[i], highs[i], lows[i], closes[i]):
                pattern_counts['doji'] += 1

            if i >= 2:
                if self._is_morning_star(df, i):
                    pattern_counts['morning_star'] += 1
                elif self._is_evening_star(df, i):
                    pattern_counts['evening_star'] += 1

        return pattern_counts
