"""
Portfolio management for the backtesting system
"""
import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from .position import Position, PositionType, PositionStatus, ExitReason
from .config import BacktestConfig

class Portfolio:
    """Portfolio management class"""

    def __init__(self, config: BacktestConfig):
        """Initialize portfolio with configuration"""
        self.config = config
        self.initial_capital = config.initial_capital
        self.cash = config.initial_capital
        self.positions: Dict[str, Position] = {}  # symbol -> position
        self.closed_positions: List[Position] = []

    def has_position_in_stock(self, symbol: str) -> bool:
        """Check if we already have an open position in a specific stock"""
        return symbol in self.positions

    def open_long_position(self, symbol: str, entry_date: datetime,
                          entry_price: float, volume: float) -> bool:
        """Open a long position"""
        # Check if we already have a position in this stock
        if self.has_position_in_stock(symbol):
            return False

        # Calculate position size
        position_value = self.initial_capital * self.config.position_size_pct
        shares = position_value / entry_price

        # Create position
        position = Position(
            symbol=symbol,
            position_type=PositionType.LONG,
            entry_date=entry_date,
            entry_price=entry_price,
            shares=shares,
            entry_value=position_value,
            stop_loss_pct=self.config.stop_loss_pct,
            stop_win_pct=self.config.stop_win_pct
        )

        # Update portfolio
        self.positions[symbol] = position

        return True

    def open_short_position(self, symbol: str, entry_date: datetime,
                           entry_price: float, volume: float) -> bool:
        """Open a short position"""
        # Check if we already have a position in this stock
        if self.has_position_in_stock(symbol):
            return False

        # Calculate position size
        position_value = self.initial_capital * self.config.position_size_pct
        shares = position_value / entry_price

        # Create position
        position = Position(
            symbol=symbol,
            position_type=PositionType.SHORT,
            entry_date=entry_date,
            entry_price=entry_price,
            shares=shares,
            entry_value=position_value,
            stop_loss_pct=self.config.stop_loss_pct,
            stop_win_pct=self.config.stop_win_pct
        )

        # Update portfolio
        self.positions[symbol] = position

        return True

    def close_position(self, symbol: str, exit_date: datetime,
                      exit_price: float, exit_reason: ExitReason):
        """Close a position"""
        if symbol not in self.positions:
            return

        position = self.positions[symbol]

        # Calculate commission
        commission = (position.entry_value * self.config.commission_bps) / 10000

        # Close position
        position.close_position(exit_date, exit_price, exit_reason, commission)

        # Move to closed positions
        self.closed_positions.append(position)
        del self.positions[symbol]

    def get_total_value(self, current_prices: Dict[str, float]) -> float:
        """Calculate total portfolio value including open positions"""
        total_value = 0

        for symbol, position in self.positions.items():
            if symbol in current_prices:
                current_price = current_prices[symbol]
                if position.position_type == PositionType.LONG:
                    total_value += position.shares * current_price
                else:  # SHORT
                    total_value += position.entry_value - (position.shares * current_price)

        return total_value

    def get_open_positions_count(self) -> Tuple[int, int]:
        """Get count of open long and short positions"""
        long_count = sum(1 for p in self.positions.values()
                        if p.position_type == PositionType.LONG)
        short_count = sum(1 for p in self.positions.values()
                         if p.position_type == PositionType.SHORT)
        return long_count, short_count
