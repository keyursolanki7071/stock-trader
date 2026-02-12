from services.load_data import load_stock_data
from strategies.swing_v1 import generate_signals
from sqlalchemy import text
from config.database import engine
import pandas as pd

RISK_PER_TRADE = 0.0075  # 0.75%
CAPITAL = 100000  # paper capital

def run_daily_scan():

    with engine.connect() as conn:
        stocks = conn.execute(text("SELECT symbol FROM stocks")).fetchall()

    nifty_df = load_stock_data("^NSEI")

    today_entries = []
    today_exits = []

    for stock in stocks:

        symbol = stock.symbol

        if symbol == "^NSEI":
            continue

        df = load_stock_data(symbol)

        if len(df) < 300:
            continue

        df = generate_signals(df, nifty_df)

        latest = df.iloc[-1]

        # Entry Signal
        if latest["entry"]:
            entry_price = latest["close"]
            stop = latest["stop"]
            risk_amount = CAPITAL * RISK_PER_TRADE
            risk_per_share = entry_price - stop
            qty = risk_amount / risk_per_share

            today_entries.append({
                "symbol": symbol,
                "entry_price": round(entry_price, 2),
                "stop": round(stop, 2),
                "qty": round(qty, 2)
            })

        # Exit Signal
        if latest["exit"]:
            today_exits.append(symbol)

    return today_entries, today_exits


if __name__ == "__main__":

    entries, exits = run_daily_scan()

    print("\n===== NEW ENTRY SIGNALS =====")
    for e in entries:
        print(f"{e['symbol']} | Entry: {e['entry_price']} | Stop: {e['stop']} | Qty: {e['qty']}")

    print("\n===== EXIT SIGNALS =====")
    for symbol in exits:
        print(symbol)
