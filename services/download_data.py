import yfinance as yf
import pandas as pd
from sqlalchemy import text
from config.database import engine
from datetime import datetime

START_DATE = "2015-01-01"


def fetch_and_store(symbol, stock_id):

    print(f"Fetching data for {symbol}")

    # Dynamic end date (today)
    end_date = datetime.today().strftime("%Y-%m-%d")

    df = yf.download(
        tickers=symbol,
        start=START_DATE,
        end=end_date,
        auto_adjust=False,
        progress=False
    )

    if df.empty:
        print(f"No data for {symbol}")
        return

    df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
    df.reset_index(inplace=True)

    with engine.begin() as conn:

        # 1️⃣ Clear existing data for this stock
        conn.execute(
            text("DELETE FROM daily_prices WHERE stock_id = :stock_id"),
            {"stock_id": stock_id}
        )

        # 2️⃣ Insert fresh data
        for _, row in df.iterrows():

            conn.execute(text("""
                INSERT INTO daily_prices 
                (stock_id, date, open, high, low, close, volume)
                VALUES (:stock_id, :date, :open, :high, :low, :close, :volume)
            """), {
                "stock_id": stock_id,
                "date": row["Date"],
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": int(row["Volume"])
            })

    print(f"{symbol} updated successfully.")


def main():

    with engine.connect() as conn:
        stocks = conn.execute(text("SELECT id, symbol FROM stocks")).fetchall()

    for stock in stocks:
        fetch_and_store(stock.symbol, stock.id)


if __name__ == "__main__":
    main()
