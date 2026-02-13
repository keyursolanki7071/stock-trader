import pandas as pd
import numpy as np

from portfolio_backtest import run_portfolio_backtest as run_breakout
from strategies.momentum_rotation import run_rotation as run_rotation


INITIAL_CAPITAL = 1000000
BREAKOUT_WEIGHT = 0.5
ROTATION_WEIGHT = 0.5


def get_breakout_equity():
    final_capital, trades, equity_curve = run_breakout(
        start_date="2015-01-01",
        end_date="2026-02-12"
    )

    equity_series = pd.Series(equity_curve)
    equity_series.index = pd.date_range(
        start="2015-01-01",
        periods=len(equity_series),
        freq="B"
    )

    return equity_series


def get_rotation_equity():
    final_capital, equity_curve = run_rotation()

    equity_series = pd.Series(equity_curve)
    equity_series.index = pd.date_range(
        start="2015-01-01",
        periods=len(equity_series),
        freq="B"
    )

    return equity_series


def run_combined():

    breakout_eq = get_breakout_equity()
    rotation_eq = get_rotation_equity()

    # Align both
    df = pd.concat([breakout_eq, rotation_eq], axis=1)
    df.columns = ["breakout", "rotation"]
    df.dropna(inplace=True)

    # Normalize both to initial capital
    df["breakout_norm"] = df["breakout"] / df["breakout"].iloc[0]
    df["rotation_norm"] = df["rotation"] / df["rotation"].iloc[0]

    # Combine
    df["combined"] = (
        BREAKOUT_WEIGHT * df["breakout_norm"] +
        ROTATION_WEIGHT * df["rotation_norm"]
    ) * INITIAL_CAPITAL

    final_capital = df["combined"].iloc[-1]
    total_return = (final_capital / INITIAL_CAPITAL - 1) * 100

    # Drawdown
    rolling_max = df["combined"].cummax()
    drawdown = (df["combined"] - rolling_max) / rolling_max
    max_dd = drawdown.min() * 100

    years = 10
    cagr = ((final_capital / INITIAL_CAPITAL) ** (1 / years) - 1) * 100

    print("\n===== COMBINED PORTFOLIO RESULTS =====")
    print("Final Capital:", round(final_capital, 2))
    print("Total Return %:", round(total_return, 2))
    print("CAGR %:", round(cagr, 2))
    print("Max Drawdown %:", round(max_dd, 2))


if __name__ == "__main__":
    run_combined()
