"""
Backtesting module for candlestick pattern strategies
"""
from .engulfing_backtester import EngulfingBacktester
from .portfolio import Portfolio
from .position import Position
from .performance import PerformanceMetrics
from .signals import TradingSignal, EngulfingSignal
from .config import BacktestConfig

__all__ = [
    'EngulfingBacktester',
    'Portfolio',
    'Position',
    'PerformanceMetrics',
    'TradingSignal',
    'EngulfingSignal',
    'BacktestConfig'
]
