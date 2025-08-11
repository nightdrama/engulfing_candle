"""
Main backtesting engine for engulfing patterns
"""
from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime
import os

from .config import BacktestConfig
from .portfolio import Portfolio
from .signals import TradingSignal, EngulfingSignal
from .performance import PerformanceMetrics
from .position import ExitReason

class EngulfingBacktester:
    """Main backtesting engine for engulfing patterns"""

    def __init__(self, config: BacktestConfig, signal_generator: TradingSignal = None):
        """Initialize backtester with configuration and signal generator"""
        self.config = config
        self.signal_generator = signal_generator or EngulfingSignal()
        self.portfolio = Portfolio(config)
        self.performance = PerformanceMetrics()
        self._signals_cache = {}  # Cache for generated signals

    def run_backtest(self, data_directory: str) -> Dict:
        """
        Run backtest with chronological day-by-day processing

        Args:
            data_directory: Path to directory containing stock CSV files

        Returns:
            Dictionary containing backtest results
        """
        # Get list of all stock files
        stock_files = [f for f in os.listdir(data_directory) if f.endswith('_daily.csv')]
        stock_files.sort()  # Ensure consistent processing order

        print(f"Found {len(stock_files)} stock files to process...")

        # Load all stock data first
        all_stock_data = {}
        all_trading_days = set()

        for i, stock_file in enumerate(stock_files, 1):
            symbol = stock_file.replace('_daily.csv', '')
            print(f"Loading data for {symbol} ({i}/{len(stock_files)})...")

            stock_data = self._load_stock_data(os.path.join(data_directory, stock_file))
            all_stock_data[symbol] = stock_data

            # Add all dates to unified timeline
            all_trading_days.update(stock_data['date'].dt.date)

        # Sort trading days chronologically
        trading_days = sorted(list(all_trading_days))
        print(f"Processing {len(trading_days)} trading days from {trading_days[0]} to {trading_days[-1]}")
        print("-" * 60)

        # Process day by day
        for i, trading_day in enumerate(trading_days, 1):
            current_date = pd.Timestamp(trading_day)
            print(f"Processing day {i}/{len(trading_days)}: {current_date.strftime('%Y-%m-%d')}")

            # Check stops for all open positions on this day
            self._check_all_stops_on_date(current_date, all_stock_data)

            # Process signals for all stocks on this day
            self._process_all_signals_on_date(current_date, all_stock_data)

        # Calculate final performance
        results = self.performance.calculate_metrics()

        # Add portfolio summary
        results['portfolio'] = {
            'final_cash': self.portfolio.cash,
            'final_positions': len(self.portfolio.positions),
            'total_closed_positions': len(self.portfolio.closed_positions)
        }

        return results

    def _load_stock_data(self, file_path: str) -> pd.DataFrame:
        """Load stock OHLCV data"""
        data = pd.read_csv(file_path)
        data['date'] = pd.to_datetime(data['date'])
        return data.sort_values('date').reset_index(drop=True)

    def _check_all_stops_on_date(self, current_date: datetime, all_stock_data: Dict[str, pd.DataFrame]):
        """Check stops for all open positions on a specific date"""

        for symbol, position in list(self.portfolio.positions.items()):
            # Get price data for this symbol on this date
            stock_data = all_stock_data[symbol]
            day_data = stock_data[stock_data['date'].dt.date == current_date.date()]

            if day_data.empty:
                continue

            current_price = day_data.iloc[0]['close']

            # Check stop loss
            if position.check_stop_loss(current_price):
                self.portfolio.close_position(
                    symbol, current_date, current_price, ExitReason.STOP_LOSS
                )
                trade_data = position.to_dict()
                self.performance.add_trade(trade_data)
                print(f"  STOP LOSS: {symbol} closed at {current_price:.2f}")
                continue

            # Check stop win
            if position.check_stop_win(current_price):
                self.portfolio.close_position(
                    symbol, current_date, current_price, ExitReason.STOP_WIN
                )
                trade_data = position.to_dict()
                self.performance.add_trade(trade_data)
                print(f"  STOP WIN: {symbol} closed at {current_price:.2f}")
                continue

    def _process_all_signals_on_date(self, current_date: datetime, all_stock_data: Dict[str, pd.DataFrame]):
        """Process all signals for a specific date across all stocks"""

        for symbol, stock_data in all_stock_data.items():
            # Check if this stock has data for this date
            day_data = stock_data[stock_data['date'].dt.date == current_date.date()]
            if day_data.empty:
                continue

            # Generate signals for this stock (if not already done)
            if symbol not in self._signals_cache:
                self._signals_cache[symbol] = self.signal_generator.generate_signals(stock_data)

            signals = self._signals_cache[symbol]

            # Get signals for this specific date
            day_signals = signals[signals['date'].dt.date == current_date.date()]

            if day_signals.empty:
                continue

            # Process signals for this day
            for _, signal in day_signals.iterrows():
                signal_date = signal['date']
                signal_type = signal['signal_type']
                signal_price = signal['price']
                signal_volume = signal['volume']

                # Check if we already have a position in this stock
                if symbol in self.portfolio.positions:
                    current_position = self.portfolio.positions[symbol]

                    # Check if this signal is opposite to our current position
                    if (current_position.position_type.value == 'long' and signal_type == 'bearish') or \
                       (current_position.position_type.value == 'short' and signal_type == 'bullish'):

                        # Close position due to opposite pattern
                        self.portfolio.close_position(
                            symbol, signal_date, signal_price, ExitReason.PATTERN_EXIT
                        )
                        trade_data = current_position.to_dict()
                        self.performance.add_trade(trade_data)
                        print(f"  PATTERN EXIT: {symbol} closed at {signal_price:.2f}")

                # Check if we can open a new position (only if no existing position)
                elif not self.portfolio.has_position_in_stock(symbol):
                    if signal_type == 'bullish':
                        if self.portfolio.open_long_position(symbol, signal_date, signal_price, signal_volume):
                            print(f"  OPENED LONG: {symbol} at {signal_price:.2f}")
                    elif signal_type == 'bearish':
                        if self.portfolio.open_short_position(symbol, signal_date, signal_price, signal_volume):
                            print(f"  OPENED SHORT: {symbol} at {signal_price:.2f}")

    def get_portfolio_summary(self) -> Dict:
        """Get current portfolio summary"""
        long_count, short_count = self.portfolio.get_open_positions_count()

        return {
            'cash': self.portfolio.cash,
            'open_long_positions': long_count,
            'open_short_positions': short_count,
            'total_open_positions': len(self.portfolio.positions),
            'closed_positions': len(self.portfolio.closed_positions)
        }
