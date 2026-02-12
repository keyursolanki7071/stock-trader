import pandas as pd
from sqlalchemy import text
from config.database import engine

def load_stock_data(symbol):
    query = """
        SELECT dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume
        FROM daily_prices dp
        JOIN stocks s ON dp.stock_id = s.id
        WHERE s.symbol = :symbol
        ORDER BY dp.date
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params={"symbol": symbol})

    df.set_index("date", inplace=True)
    return df
