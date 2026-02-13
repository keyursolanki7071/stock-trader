from services.load_data import load_stock_data
from strategies.trend_breakout import generate_signals
from sqlalchemy import text
from config.database import engine
import pandas as pd
import numpy as np

INITIAL_CAPITAL = 100000
RISK_PER_TRADE = 0.01
MAX_PORTFOLIO_RISK = 0.04


def prepare_master_dataframe():

    master_df = []

    with engine.connect() as conn:
        stocks = conn.execute(text("SELECT symbol FROM stocks")).fetchall()

    nifty_df = load_stock_data("^NSEI")

    for stock in stocks:

        symbol = stock.symbol

        if symbol == "^NSEI":
            continue

        df = load_stock_data(symbol)

        if len(df) < 300:
            continue

        df = generate_signals(df, nifty_df)
        df["symbol"] = symbol

        master_df.append(df)

    master_df = pd.concat(master_df)
    master_df.sort_index(inplace=True)
    master_df.index = pd.to_datetime(master_df.index)


    return master_df


def run_portfolio_backtest(start_date=None, end_date=None):

    df = prepare_master_dataframe()

    if start_date:
        df = df[df.index >= pd.to_datetime(start_date)]

    if end_date:
        df = df[df.index <= pd.to_datetime(end_date)]

    capital = INITIAL_CAPITAL
    equity_curve = []
    open_positions = {}
    trades = []

    unique_dates = sorted(df.index.unique())

    for current_date in unique_dates:

        if capital <= 0:
            print("Capital depleted.")
            break

        daily_data = df.loc[current_date]

        if isinstance(daily_data, pd.Series):
            daily_data = daily_data.to_frame().T

        # ==========================
        # 1️⃣ PROCESS EXITS
        # ==========================
        for symbol in list(open_positions.keys()):

            row = daily_data[daily_data["symbol"] == symbol]

            if row.empty:
                continue

            row = row.iloc[0]

            entry_price = open_positions[symbol]["entry_price"]
            qty = open_positions[symbol]["qty"]
            stop = open_positions[symbol]["stop"]

            exit_price = None

            # Stop loss
            if row["low"] <= stop:
                exit_price = stop * 0.998

            # Strategy exit
            elif row["exit"]:
                exit_price = row["close"] * 0.998

            if exit_price is not None:

                pnl = (exit_price - entry_price) * qty
                capital += pnl
                trades.append(pnl)

                del open_positions[symbol]

        # ==========================
        # 2️⃣ PROCESS ENTRIES
        # ==========================
        current_risk = sum(pos["risk_amount"] for pos in open_positions.values())

        for _, row in daily_data.iterrows():

            if not row["entry"]:
                continue

            if row["symbol"] in open_positions:
                continue

            if current_risk >= capital * MAX_PORTFOLIO_RISK:
                break

            entry_price = row["close"] * 1.002
            stop = row["stop"]

            # Validate stop
            if pd.isna(stop):
                continue

            risk_per_share = entry_price - stop

            # Validate risk
            if risk_per_share <= 0 or pd.isna(risk_per_share):
                continue

            risk_amount = capital * RISK_PER_TRADE
            qty = risk_amount / risk_per_share

            # Validate qty
            if qty <= 0 or pd.isna(qty) or np.isinf(qty):
                continue

            open_positions[row["symbol"]] = {
                "entry_price": entry_price,
                "qty": qty,
                "stop": stop,
                "risk_amount": risk_amount,
                "entry_date" : current_date
            }

            current_risk += risk_amount

        equity_curve.append(capital)
    return capital, trades, equity_curve

def print_results(title, final_capital, trades, equity_curve):

    trades = np.array(trades)

    if len(trades) == 0:
        print(f"\n===== {title} =====")
        print("No trades generated.")
        return

    win_trades = trades[trades > 0]
    loss_trades = trades[trades <= 0]

    total_trades = len(trades)
    win_rate = len(win_trades) / total_trades if total_trades else 0
    profit_factor = abs(win_trades.sum() / loss_trades.sum()) if len(loss_trades) else 0
    total_return = (final_capital / INITIAL_CAPITAL - 1) * 100

    equity_series = pd.Series(equity_curve)
    rolling_max = equity_series.cummax()
    drawdown = (equity_series - rolling_max) / rolling_max
    max_drawdown = drawdown.min() * 100

    print(f"\n===== {title} =====")
    print("Total Trades:", total_trades)
    print("Win Rate:", round(win_rate, 2))
    print("Profit Factor:", round(profit_factor, 2))
    print("Final Capital:", round(final_capital, 2))
    print("Total Return %:", round(total_return, 2))
    print("Max Drawdown %:", round(max_drawdown, 2))



if __name__ == "__main__":

    # =============================
    # IN-SAMPLE (2015–2019)
    # =============================
    # is_capital, is_trades, is_equity = run_portfolio_backtest(
    #     start_date="2015-01-01",
    #     end_date="2019-12-31"
    # )

    # print_results("IN-SAMPLE (2015–2019)", is_capital, is_trades, is_equity)


    # # =============================
    # # OUT-OF-SAMPLE (2020–2024)
    # # =============================
    # oos_capital, oos_trades, oos_equity = run_portfolio_backtest(
    #     start_date="2020-01-01",
    #     end_date="2024-12-31"
    # )

    final_capital, trades, equity_curve = run_portfolio_backtest()

    print_results("BACKTEST RESULTS", final_capital, trades, equity_curve)

    # final_capital, trades, equity_curve = run_portfolio_backtest()

    # trades = np.array(trades)

    # if len(trades) == 0:
    #     print("No trades generated.")
    #     exit()

    # win_trades = trades[trades > 0]
    # loss_trades = trades[trades <= 0]

    # total_trades = len(trades)
    # win_rate = len(win_trades) / total_trades if total_trades else 0
    # profit_factor = abs(win_trades.sum() / loss_trades.sum()) if len(loss_trades) else 0
    # total_return = (final_capital / INITIAL_CAPITAL - 1) * 100

    # # Max Drawdown
    # equity_series = pd.Series(equity_curve)
    # rolling_max = equity_series.cummax()
    # drawdown = (equity_series - rolling_max) / rolling_max
    # max_drawdown = drawdown.min() * 100

    # print("\n====== PORTFOLIO RESULTS ======")
    # print("Total Trades:", total_trades)
    # print("Win Rate:", round(win_rate, 2))
    # print("Profit Factor:", round(profit_factor, 2))
    # print("Final Capital:", round(final_capital, 2))
    # print("Total Return %:", round(total_return, 2))
    # print("Max Drawdown %:", round(max_drawdown, 2))