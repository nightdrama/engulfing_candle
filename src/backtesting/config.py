"""
Configuration parameters for the backtesting system
"""
from dataclasses import dataclass

@dataclass
class BacktestConfig:
    """Configuration class for backtesting parameters"""

    # Portfolio settings
    initial_capital: float = 1_000_000.0  # $1M starting capital
    position_size_pct: float = 0.05       # 5% per position

    # Risk management
    stop_loss_pct: float = 0.05           # 5% stop loss
    stop_win_pct: float = 0.20            # 20% stop win

    # Trading costs
    commission_bps: float = 10.0          # 10 basis points commission
