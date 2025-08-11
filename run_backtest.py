"""
Main script to run the engulfing pattern backtest
"""
from src.backtesting.config import BacktestConfig
from src.backtesting.signals import EngulfingSignal
from src.backtesting.engulfing_backtester import EngulfingBacktester
import os

def main():
    # Load configuration
    config = BacktestConfig(
        initial_capital=1_000_000.0,
        position_size_pct=0.05,
        stop_loss_pct=0.05,
        stop_win_pct=0.20,
        commission_bps=10
    )

    # Create signal generator
    signal_generator = EngulfingSignal()

    # Create and run backtester
    backtester = EngulfingBacktester(config, signal_generator)

    print("Starting Engulfing Pattern Backtest...")
    print(f"Initial Capital: ${config.initial_capital:,.0f}")
    print(f"Position Size: {config.position_size_pct*100:.1f}%")
    print(f"Stop Loss: {config.stop_loss_pct*100:.1f}%")
    print(f"Stop Win: {config.stop_win_pct*100:.1f}%")
    print(f"Commission: {config.commission_bps} bps")
    print("-" * 60)

    results = backtester.run_backtest('data/temp/')

    # Create output directory
    output_dir = 'data/results/engulfing_pattern'
    os.makedirs(output_dir, exist_ok=True)

    # Export detailed results
    backtester.performance.export_detailed_results(output_dir, results)

    # Print comprehensive results
    print("\n" + "=" * 60)
    print("ENGULFING PATTERN BACKTEST RESULTS")
    print("=" * 60)


    # Long position performance
    long_metrics = results['long_positions']
    print(f"\nLONG POSITIONS:")
    print(f"Hit Rate: {long_metrics['hit_rate']:.1f}% ({long_metrics['profitable_trades']}/{long_metrics['total_trades']})")
    print(f"Average Return: {long_metrics['average_return']:.2f}%")
    print(f"Total Return: {long_metrics['total_return']:.2f}%")
    print(f"Best Trade: {long_metrics['best_trade']:.2f}%")
    print(f"Worst Trade: {long_metrics['worst_trade']:.2f}%")

    # Short position performance
    short_metrics = results['short_positions']
    print(f"\nSHORT POSITIONS:")
    print(f"Hit Rate: {short_metrics['hit_rate']:.1f}% ({short_metrics['profitable_trades']}/{short_metrics['total_trades']})")
    print(f"Average Return: {short_metrics['average_return']:.2f}%")
    print(f"Total Return: {short_metrics['total_return']:.2f}%")
    print(f"Best Trade: {short_metrics['best_trade']:.2f}%")
    print(f"Worst Trade: {short_metrics['worst_trade']:.2f}%")

    # Combined metrics
    combined = results['combined']
    print(f"\nCOMBINED METRICS:")
    print(f"Overall Hit Rate: {combined['hit_rate']:.1f}%")
    print(f"Overall Average Return: {combined['average_return']:.2f}%")

    # Portfolio summary
    portfolio = results['portfolio']
    print(f"\nPORTFOLIO SUMMARY:")
    print(f"Final Cash: ${portfolio['final_cash']:,.2f}")
    print(f"Open Positions: {portfolio['final_positions']}")
    print(f"Closed Positions: {portfolio['total_closed_positions']}")

    print(f"\nDetailed results exported to: {output_dir}")
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()
