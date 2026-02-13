from services.load_data import load_stock_data
from strategies.trend_breakout import generate_signals
from sqlalchemy import text
from config.database import engine
import numpy as np
import pandas as pd


INITIAL_CAPITAL = 1000000
RISK_PER_TRADE = 0.01


def backtest_stock(df, capital):

    trades = []
    in_position = False

    for i in range(1, len(df)):

        # =====================
        # ENTRY
        # =====================
        if not in_position and df["entry"].iloc[i]:

            entry_price = df["close"].iloc[i] * 1.002  # slippage
            stop = df["stop"].iloc[i]

            if pd.isna(stop):
                continue

            if stop >= entry_price:
                continue

            risk_amount = capital * RISK_PER_TRADE
            qty = risk_amount / (entry_price - stop)

            in_position = True

        # =====================
        # EXIT
        # =====================
        elif in_position and df["exit"].iloc[i]:

            exit_price = df["close"].iloc[i] * 0.998  # slippage

            pnl = (exit_price - entry_price) * qty
            capital += pnl
            trades.append(pnl)

            in_position = False

    return capital, trades


def run_backtest():

    capital = INITIAL_CAPITAL
    all_trades = []
    nifty_df = load_stock_data("^NSEI")


    with engine.connect() as conn:
        stocks = conn.execute(text("SELECT symbol FROM stocks")).fetchall()

    for stock in stocks:

        symbol = stock.symbol
        print(f"Testing {symbol}")

        df = load_stock_data(symbol)

        if len(df) < 300:
            continue

        df = generate_signals(df, nifty_df)

        capital, trades = backtest_stock(df, capital)
        all_trades.extend(trades)

    return capital, all_trades


if __name__ == "__main__":

    final_capital, trades = run_backtest()

    if len(trades) == 0:
        print("No trades generated")
        exit()

    trades = np.array(trades)

    winning_trades = trades[trades > 0]
    losing_trades = trades[trades <= 0]

    total_trades = len(trades)
    win_count = len(winning_trades)
    loss_count = len(losing_trades)

    win_rate = win_count / total_trades

    avg_win = np.mean(winning_trades) if win_count > 0 else 0
    avg_loss = np.mean(losing_trades) if loss_count > 0 else 0

    profit_factor = abs(winning_trades.sum() / losing_trades.sum()) if loss_count > 0 else 0

    expectancy = (win_rate * avg_win) + ((1 - win_rate) * avg_loss)

    total_return = (final_capital / INITIAL_CAPITAL - 1) * 100

    print("\n====== RESULTS ======")
    print("Total Trades:", total_trades)
    print("Winning Trades:", win_count)
    print("Losing Trades:", loss_count)
    print("Win Rate:", round(win_rate, 2))
    print("Final Capital:", round(final_capital, 2))
    print("Total Return %:", round(total_return, 2))
    print("Average Win:", round(avg_win, 2))
    print("Average Loss:", round(avg_loss, 2))
    print("Profit Factor:", round(profit_factor, 2))
    print("Expectancy per Trade:", round(expectancy, 2))

