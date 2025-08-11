"""
Performance metrics calculation for the backtesting system
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from datetime import datetime
import os

class PerformanceMetrics:
    """Performance metrics calculator"""

    def __init__(self):
        self.trades: List[Dict] = []
        self.daily_returns: List[float] = []
        self.daily_dates: List[datetime] = []

    def add_trade(self, trade: Dict):
        """Add a completed trade to the performance tracking"""
        self.trades.append(trade)

    def add_daily_return(self, date: datetime, return_pct: float):
        """Add daily portfolio return"""
        self.daily_dates.append(date)
        self.daily_returns.append(return_pct)

    def export_detailed_results(self, output_dir: str, results: Dict):
        """Export detailed results to CSV files"""
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Export all trades to CSV
        if self.trades:
            trades_df = pd.DataFrame(self.trades)
            trades_df.to_csv(os.path.join(output_dir, 'all_trades.csv'), index=False)

            # Separate long and short trades
            long_trades = trades_df[trades_df['position_type'] == 'long']
            short_trades = trades_df[trades_df['position_type'] == 'short']

            if not long_trades.empty:
                long_trades.to_csv(os.path.join(output_dir, 'long_trades.csv'), index=False)
            if not short_trades.empty:
                short_trades.to_csv(os.path.join(output_dir, 'short_trades.csv'), index=False)

        # Export daily returns
        if self.daily_returns:
            daily_returns_df = pd.DataFrame({
                'date': self.daily_dates,
                'daily_return_pct': self.daily_returns
            })
            daily_returns_df.to_csv(os.path.join(output_dir, 'daily_returns.csv'), index=False)

        # Export summary metrics - removed Total Return, Sharpe Ratio, Max Drawdown
        summary_data = {
            'metric': [
                'Total Trades',
                'Long Hit Rate (%)',
                'Long Average Return (%)',
                'Long Total Return (%)',
                'Short Hit Rate (%)',
                'Short Average Return (%)',
                'Short Total Return (%)',
                'Overall Hit Rate (%)',
                'Overall Average Return (%)'
            ],
            'value': [
                results.get('overall', {}).get('total_trades', 0),
                results.get('long_positions', {}).get('hit_rate', 0.0),
                results.get('long_positions', {}).get('average_return', 0.0),
                results.get('long_positions', {}).get('total_return', 0.0),
                results.get('short_positions', {}).get('hit_rate', 0.0),
                results.get('short_positions', {}).get('average_return', 0.0),
                results.get('short_positions', {}).get('total_return', 0.0),
                results.get('combined', {}).get('hit_rate', 0.0),
                results.get('combined', {}).get('average_return', 0.0)
            ]
        }

        summary_df = pd.DataFrame(summary_data)
        summary_df.to_csv(os.path.join(output_dir, 'summary_metrics.csv'), index=False)

        # Export trade statistics by symbol
        if self.trades:
            symbol_stats = self._calculate_symbol_statistics()
            symbol_stats_df = pd.DataFrame(symbol_stats)
            symbol_stats_df.to_csv(os.path.join(output_dir, 'symbol_statistics.csv'), index=False)

    def _calculate_symbol_statistics(self) -> List[Dict]:
        """Calculate trading statistics for each symbol"""
        if not self.trades:
            return []

        trades_df = pd.DataFrame(self.trades)
        symbol_stats = []

        for symbol in trades_df['symbol'].unique():
            symbol_trades = trades_df[trades_df['symbol'] == symbol]

            # Calculate metrics for this symbol
            total_trades = len(symbol_trades)
            profitable_trades = len(symbol_trades[symbol_trades['return_pct'] > 0])
            hit_rate = (profitable_trades / total_trades) * 100 if total_trades > 0 else 0.0
            average_return = symbol_trades['return_pct'].mean()
            total_return = symbol_trades['return_pct'].sum()
            best_trade = symbol_trades['return_pct'].max()
            worst_trade = symbol_trades['return_pct'].min()

            symbol_stats.append({
                'symbol': symbol,
                'total_trades': total_trades,
                'profitable_trades': profitable_trades,
                'hit_rate': hit_rate,
                'average_return': average_return,
                'total_return': total_return,
                'best_trade': best_trade,
                'worst_trade': worst_trade
            })

        return symbol_stats

    def calculate_metrics(self) -> Dict:
        """Calculate comprehensive performance metrics"""
        if not self.trades:
            return self._empty_results()

        # Separate long and short trades
        long_trades = [t for t in self.trades if t['position_type'] == 'long']
        short_trades = [t for t in self.trades if t['position_type'] == 'short']

        # Long position metrics
        long_metrics = self._calculate_position_metrics(long_trades, 'long')

        # Short position metrics
        short_metrics = self._calculate_position_metrics(short_trades, 'short')

        return {
            'overall': {
                'total_trades': len(self.trades)
            },
            'long_positions': long_metrics,
            'short_positions': short_metrics,
            'combined': {
                'hit_rate': self._calculate_overall_hit_rate(),
                'average_return': self._calculate_overall_average_return()
            }
        }

    def _calculate_position_metrics(self, trades: List[Dict], position_type: str) -> Dict:
        """Calculate metrics for specific position type (long/short)"""
        if not trades:
            return {
                'hit_rate': 0.0,
                'average_return': 0.0,
                'total_trades': 0,
                'profitable_trades': 0,
                'total_return': 0.0,
                'best_trade': 0.0,
                'worst_trade': 0.0
            }

        # Calculate returns for each trade
        returns = [t['return_pct'] for t in trades]

        # Hit rate (percentage of profitable trades)
        profitable_trades = sum(1 for r in returns if r > 0)
        hit_rate = (profitable_trades / len(trades)) * 100

        # Average return
        average_return = np.mean(returns)

        # Total return for this position type
        total_return = sum(returns)

        # Best and worst trades
        best_trade = max(returns)
        worst_trade = min(returns)

        return {
            'hit_rate': hit_rate,
            'average_return': average_return,
            'total_trades': len(trades),
            'profitable_trades': profitable_trades,
            'total_return': total_return,
            'best_trade': best_trade,
            'worst_trade': worst_trade
        }

    def _calculate_overall_hit_rate(self) -> float:
        """Calculate overall hit rate across all trades"""
        if not self.trades:
            return 0.0

        profitable_trades = sum(1 for t in self.trades if t['return_pct'] > 0)
        return (profitable_trades / len(self.trades)) * 100

    def _calculate_overall_average_return(self) -> float:
        """Calculate overall average return across all trades"""
        if not self.trades:
            return 0.0

        returns = [t['return_pct'] for t in self.trades]
        return np.mean(returns)

    def _empty_results(self) -> Dict:
        """Return empty results structure"""
        return {
            'overall': {
                'total_trades': 0
            },
            'long_positions': {
                'hit_rate': 0.0,
                'average_return': 0.0,
                'total_trades': 0,
                'profitable_trades': 0,
                'total_return': 0.0,
                'best_trade': 0.0,
                'worst_trade': 0.0
            },
            'short_positions': {
                'hit_rate': 0.0,
                'average_return': 0.0,
                'total_trades': 0,
                'profitable_trades': 0,
                'total_return': 0.0,
                'best_trade': 0.0,
                'worst_trade': 0.0
            },
            'combined': {
                'hit_rate': 0.0,
                'average_return': 0.0
            }
        }
