import pandas as pd
import numpy as np
from services.load_data import load_stock_data
from sqlalchemy import text
from config.database import engine


INITIAL_CAPITAL = 500000
TOP_N = 10

def prepare_data():

    master = []

    with engine.connect() as conn:
        stocks = conn.execute(text("SELECT symbol FROM stocks")).fetchall()

    nifty_df = load_stock_data("^NSEI")
    nifty_df.index = pd.to_datetime(nifty_df.index)
    nifty_df["ema_200"] = nifty_df["close"].ewm(span=200, adjust=False).mean()

    for stock in stocks:

        symbol = stock.symbol

        if symbol == "^NSEI":
            continue

        df = load_stock_data(symbol)
        df.index = pd.to_datetime(df.index)

        if len(df) < 200:
            continue

        df["ret_63"] = df["close"] / df["close"].shift(63) - 1
        df["symbol"] = symbol

        master.append(df[["close", "ret_63", "symbol"]])

    master_df = pd.concat(master)
    master_df.sort_index(inplace=True)

    return master_df, nifty_df


def get_month_end_dates(df):
    return df.resample("ME").last().index


def run_rotation():

    master_df, nifty_df = prepare_data()

    capital = INITIAL_CAPITAL
    equity_curve = []
    current_positions = {}

    month_end_dates = get_month_end_dates(master_df)

    all_dates = sorted(master_df.index.unique())

    for i, date in enumerate(all_dates):

        # ==========================
        # Rebalance at month end
        # ==========================
        if date in month_end_dates:

            # Mark-to-market current portfolio
            portfolio_value = 0

            if current_positions:
                day_data = master_df.loc[date]
                if isinstance(day_data, pd.DataFrame):
                    for symbol, pos in current_positions.items():
                        row = day_data[day_data["symbol"] == symbol]
                        if not row.empty:
                            portfolio_value += row.iloc[0]["close"] * pos["qty"]

                capital = portfolio_value

            # Regime Filter
            if date in nifty_df.index:
                if nifty_df.loc[date, "close"] <= nifty_df.loc[date, "ema_200"]:
                    current_positions = {}
                    equity_curve.append(capital)
                    continue

            # Rank stocks
            daily_data = master_df.loc[date]
            if isinstance(daily_data, pd.Series):
                continue

            ranked = daily_data.sort_values("ret_63", ascending=False)
            top_stocks = ranked.head(TOP_N)

            current_positions = {}

            allocation = capital / TOP_N

            for _, row in top_stocks.iterrows():
                qty = allocation / row["close"]
                current_positions[row["symbol"]] = {
                    "qty": qty
                }

        # ==========================
        # Daily mark-to-market
        # ==========================
        portfolio_value = 0

        if current_positions:
            day_data = master_df.loc[date]
            if isinstance(day_data, pd.DataFrame):
                for symbol, pos in current_positions.items():
                    row = day_data[day_data["symbol"] == symbol]
                    if not row.empty:
                        portfolio_value += row.iloc[0]["close"] * pos["qty"]
        else:
            portfolio_value = capital

        equity_curve.append(portfolio_value)

    final_capital = equity_curve[-1]
    total_return = (final_capital / INITIAL_CAPITAL - 1) * 100

    equity_series = pd.Series(equity_curve)
    rolling_max = equity_series.cummax()
    drawdown = (equity_series - rolling_max) / rolling_max
    max_dd = drawdown.min() * 100

    return final_capital, equity_curve
    print("\n===== MOMENTUM ROTATION V2 RESULTS =====")
    print("Final Capital:", round(final_capital, 2))
    print("Total Return %:", round(total_return, 2))
    print("Max Drawdown %:", round(max_dd, 2))


if __name__ == "__main__":
    run_rotation()
