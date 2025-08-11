"""
Position tracking for individual stock positions
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Literal
from enum import Enum

class PositionType(Enum):
    """Position type enumeration"""
    LONG = "long"
    SHORT = "short"

class PositionStatus(Enum):
    """Position status enumeration"""
    OPEN = "open"
    CLOSED = "closed"

class ExitReason(Enum):
    """Exit reason enumeration"""
    PATTERN_EXIT = "pattern_exit"
    STOP_LOSS = "stop_loss"
    STOP_WIN = "stop_win"

@dataclass
class Position:
    """Individual stock position"""

    symbol: str
    position_type: PositionType
    entry_date: datetime
    entry_price: float
    shares: float
    entry_value: float
    stop_loss_pct: float
    stop_win_pct: float

    # Exit information (filled when position is closed)
    exit_date: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_value: Optional[float] = None
    return_pct: Optional[float] = None
    return_amount: Optional[float] = None
    hold_days: Optional[int] = None
    exit_reason: Optional[ExitReason] = None
    commission: Optional[float] = None

    def __post_init__(self):
        """Calculate derived fields"""
        self.status = PositionStatus.OPEN

    def close_position(self, exit_date: datetime, exit_price: float,
                      exit_reason: ExitReason, commission: float):
        """Close the position and calculate returns"""
        self.exit_date = exit_date
        self.exit_price = exit_price
        self.exit_value = self.shares * exit_price
        self.commission = commission

        # Calculate returns
        if self.position_type == PositionType.LONG:
            self.return_amount = self.exit_value - self.entry_value - commission
            self.return_pct = (self.return_amount / self.entry_value) * 100
        else:  # SHORT
            self.return_amount = self.entry_value - self.exit_value - commission
            self.return_pct = (self.return_amount / self.entry_value) * 100

        # Calculate hold days
        self.hold_days = (exit_date - self.entry_date).days
        self.exit_reason = exit_reason
        self.status = PositionStatus.CLOSED

    def check_stop_loss(self, current_price: float) -> bool:
        """Check if stop loss has been hit"""
        if self.position_type == PositionType.LONG:
            return current_price <= self.entry_price * (1 - self.stop_loss_pct)
        else:  # SHORT
            return current_price >= self.entry_price * (1 + self.stop_loss_pct)

    def check_stop_win(self, current_price: float) -> bool:
        """Check if stop win has been hit"""
        if self.position_type == PositionType.LONG:
            return current_price >= self.entry_price * (1 + self.stop_win_pct)
        else:  # SHORT
            return current_price <= self.entry_price * (1 - self.stop_win_pct)

    def to_dict(self) -> dict:
        """Convert position to dictionary for export"""
        return {
            'symbol': self.symbol,
            'entry_date': self.entry_date.strftime('%Y-%m-%d'),
            'exit_date': self.exit_date.strftime('%Y-%m-%d') if self.exit_date else None,
            'position_type': self.position_type.value,
            'entry_price': self.entry_price,
            'exit_price': self.exit_price,
            'shares': self.shares,
            'entry_value': self.entry_value,
            'exit_value': self.exit_value,
            'return_pct': self.return_pct,
            'return_amount': self.return_amount,
            'hold_days': self.hold_days,
            'exit_reason': self.exit_reason.value if self.exit_reason else None,
            'commission': self.commission
        }
