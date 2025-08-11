"""
Trading signal interface for the backtesting system
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional
import pandas as pd
from datetime import datetime

class TradingSignal(ABC):
    """Abstract base class for trading signals"""

    @abstractmethod
    def generate_signals(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate trading signals for a given stock's OHLCV data

        Args:
            stock_data: DataFrame with columns ['date', 'open', 'high', 'low', 'close', 'volume']

        Returns:
            DataFrame with columns ['date', 'signal_type', 'price', 'volume']
            signal_type: 'bullish', 'bearish', or 'none'
        """
        pass

class EngulfingSignal(TradingSignal):
    """Engulfing pattern signal generator"""

    def __init__(self):
        from ..strategies.engulfing import EngulfingPattern
        self.engulfing_detector = EngulfingPattern()

    def generate_signals(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        """Generate engulfing pattern signals"""
        # Use the detect method to get all patterns at once
        patterns = self.engulfing_detector.detect(stock_data)
        signals = []

        # Process the patterns series
        for i in range(len(patterns)):
            if patterns.iloc[i] == 1:  # Bullish engulfing
                signals.append({
                    'date': stock_data.iloc[i]['date'],
                    'signal_type': 'bullish',
                    'price': stock_data.iloc[i]['close'],
                    'volume': stock_data.iloc[i]['volume']
                })
            elif patterns.iloc[i] == -1:  # Bearish engulfing
                signals.append({
                    'date': stock_data.iloc[i]['date'],
                    'signal_type': 'bearish',
                    'price': stock_data.iloc[i]['close'],
                    'volume': stock_data.iloc[i]['volume']
                })

        return pd.DataFrame(signals) if signals else pd.DataFrame(columns=['date', 'signal_type', 'price', 'volume'])
