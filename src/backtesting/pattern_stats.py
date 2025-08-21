"""
Pattern Statistics Utility - Simplified Version
"""
import pandas as pd
import numpy as np
from scipy.stats import ttest_1samp


def calculate_pattern_statistics(patterns_df: pd.DataFrame) -> dict:
    """Calculate pattern performance statistics"""
    results = {}
    return_cols = ['forward_1d_return', 'forward_5d_return', 'forward_10d_return']

    for pattern in patterns_df['pattern_name'].unique():
        pattern_data = patterns_df[patterns_df['pattern_name'] == pattern]
        pattern_stats = {}

        for col in return_cols:
            if col in pattern_data.columns:
                returns = pattern_data[col].dropna()
                if len(returns) > 0:
                    t_stat, p_value = ttest_1samp(returns, 0)
                    pattern_stats[col] = {
                        'mean': returns.mean(), 'std': returns.std(),
                        't_stat': t_stat, 'p_value': p_value,
                        'hit_rate': (returns > 0).mean(), 'n_obs': len(returns)
                    }
                else:
                    pattern_stats[col] = {'mean': np.nan, 'std': np.nan, 't_stat': np.nan,
                                        'p_value': np.nan, 'hit_rate': np.nan, 'n_obs': 0}
        results[pattern] = pattern_stats
    return results


def print_summary(statistics: dict) -> None:
    """Print formatted pattern statistics summary"""
    print("=" * 60)
    print("PATTERN PERFORMANCE SUMMARY")
    print("=" * 60)

    for pattern, stats in statistics.items():
        print(f"\n{pattern.upper()}:")
        for time_horizon, metrics in stats.items():
            if metrics['n_obs'] > 0:
                print(f"  {time_horizon}: Mean={metrics['mean']:.4f}, "
                      f"Hit Rate={metrics['hit_rate']:.3f}, T-Stat={metrics['t_stat']:.3f}, Obs={metrics['n_obs']}")


def get_best_patterns(statistics: dict, metric: str = 'mean', time_horizon: str = 'forward_1d_return') -> list:
    """Get best performing patterns by metric"""
    pattern_scores = []
    for pattern, stats in statistics.items():
        if time_horizon in stats and stats[time_horizon]['n_obs'] > 0:
            value = stats[time_horizon][metric]
            pattern_scores.append((pattern, value))

    reverse = metric in ['mean', 'hit_rate']
    pattern_scores.sort(key=lambda x: abs(x[1]) if metric == 't_stat' else x[1], reverse=reverse)
    return pattern_scores


def calculate_aggregate_stats(patterns_df: pd.DataFrame) -> dict:
    """Calculate aggregate statistics across all patterns"""
    agg_stats = {}
    return_cols = ['forward_1d_return', 'forward_5d_return', 'forward_10d_return']

    for col in return_cols:
        if col in patterns_df.columns:
            returns = patterns_df[col].dropna()
            if len(returns) > 0:
                t_stat, p_value = ttest_1samp(returns, 0)
                agg_stats[col] = {
                    'mean': returns.mean(), 'std': returns.std(),
                    't_stat': t_stat, 'p_value': p_value,
                    'hit_rate': (returns > 0).mean(), 'n_obs': len(returns)
                }
    return agg_stats


# Example usage
if __name__ == "__main__":
    sample_data = pd.DataFrame({
        'pattern_name': ['hammer', 'doji', 'hammer'],
        'forward_1d_return': [0.02, -0.01, 0.03],
        'forward_5d_return': [0.05, -0.02, 0.08],
        'forward_10d_return': [0.10, -0.05, 0.15]
    })

    stats = calculate_pattern_statistics(sample_data)
    print_summary(stats)
    best = get_best_patterns(stats, 'mean', 'forward_1d_return')
    print(f"\nBest patterns: {best}")
    agg = calculate_aggregate_stats(sample_data)
    print(f"Aggregate 1-day return: {agg['forward_1d_return']['mean']:.4f}")
