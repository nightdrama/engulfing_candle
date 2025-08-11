"""
Abstract base class for candlestick pattern detection
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np

class BasePattern(ABC):
    """Abstract base class for all candlestick patterns"""

    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description

    @abstractmethod
    def detect(self, df: pd.DataFrame) -> pd.Series:
        """
        Detect pattern in the given DataFrame

        Args:
            df: DataFrame with columns: date, open, high, low, close, volume

        Returns:
            pd.Series with boolean values indicating pattern presence
        """
        pass

    @abstractmethod
    def get_signal(self, df: pd.DataFrame) -> pd.Series:
        """
        Get trading signal based on pattern detection

        Args:
            df: DataFrame with OHLCV data

        Returns:
            pd.Series with signal values: 1 (buy), -1 (sell), 0 (no signal)
        """
        pass

    def validate_data(self, df: pd.DataFrame) -> bool:
        """
        Validate that DataFrame has required columns and data

        Args:
            df: DataFrame to validate

        Returns:
            True if valid, False otherwise
        """
        required_columns = ['open', 'high', 'low', 'close', 'volume']

        if not all(col in df.columns for col in required_columns):
            print(f"Missing required columns. Need: {required_columns}")
            return False

        if len(df) < 2:
            print("DataFrame must have at least 2 rows for pattern detection")
            return False

        if df.isnull().any().any():
            print("DataFrame contains null values")
            return False

        return True

    def get_pattern_info(self) -> Dict[str, str]:
        """
        Get pattern information

        Returns:
            Dictionary with pattern name and description
        """
        return {
            'name': self.name,
            'description': self.description
        }
