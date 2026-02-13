import pandas as pd
from sqlalchemy import text
from config.database import engine
from services.load_data import load_stock_data
from datetime import datetime

# AT MONTH END AFTER MARKET CLOSE
TOP_N = 10
CAPITAL = 100000   # Your capital
DEPLOYMENT_RATIO = 1.0   # Change to 0.7 if desired


def is_month_end(date, df):
    month_ends = df.resample("ME").last().index
    return date in month_ends


def run_rotation_signal():

    # today = pd.Timestamp(datetime.today().date())
    today = "2026-01-30 00:00:00"

    with engine.connect() as conn:
        stocks = conn.execute(text("SELECT symbol FROM stocks")).fetchall()

    # Load NIFTY
    nifty_df = load_stock_data("^NSEI")
    nifty_df.index = pd.to_datetime(nifty_df.index)
    nifty_df["ema_200"] = nifty_df["close"].ewm(span=200, adjust=False).mean()

    if today not in nifty_df.index:
        print("No market data for today.")
        return

    # Check regime
    if nifty_df.loc[today, "close"] <= nifty_df.loc[today, "ema_200"]:
        print("\n===== ROTATION SIGNAL =====")
        print("Market regime OFF (NIFTY below EMA200)")
        print("Stay in CASH.")
        return

    master = []

    for stock in stocks:

        symbol = stock.symbol

        if symbol == "^NSEI":
            continue

        df = load_stock_data(symbol)
        df.index = pd.to_datetime(df.index)

        if len(df) < 70:
            continue

        if today not in df.index:
            continue

        df["ret_63"] = df["close"] / df["close"].shift(63) - 1

        ret_63 = df.loc[today, "ret_63"]
        price = df.loc[today, "close"]

        if pd.isna(ret_63):
            continue

        master.append({
            "symbol": symbol,
            "ret_63": ret_63,
            "price": price
        })

    if not master:
        print("No valid data.")
        return

    ranking_df = pd.DataFrame(master)
    ranking_df.sort_values("ret_63", ascending=False, inplace=True)

    top_stocks = ranking_df.head(TOP_N)

    deployable_capital = CAPITAL * DEPLOYMENT_RATIO
    allocation = deployable_capital / TOP_N

    print("\n===== MONTHLY MOMENTUM ROTATION SIGNAL =====")
    print(f"Date: {today}")
    print(f"Top {TOP_N} Stocks:\n")

    for _, row in top_stocks.iterrows():

        qty = allocation / row["price"]

        print(
            f"{row['symbol']} | "
            f"63D Return: {round(row['ret_63'] * 100, 2)}% | "
            f"Price: {round(row['price'], 2)} | "
            f"Qty: {int(qty)}"
        )


if __name__ == "__main__":
    run_rotation_signal()
