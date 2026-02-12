from sqlalchemy import text
from config.database import engine

symbols = [
    "RELIANCE.NS",
    "HDFCBANK.NS",
    "INFY.NS",
    "ICICIBANK.NS",
    "TCS.NS",
    "BHARTIARTL.NS",
    "AXISBANK.NS",
    "SBIN.NS",
    "BAJFINANCE.NS",
    "HDFC.NS",
    "LT.NS",
    "ITC.NS",
    "KOTAKBANK.NS",
    "MARUTI.NS",
    "HINDUNILVR.NS",
    "BHARATPE.NS",  # may vary or be placeholder depending on data source
    "SUNPHARMA.NS",
    "ULTRATECH.NS",
    "WIPRO.NS",
    "ADANIENT.NS",
    "TITAN.NS",
    "ONGC.NS",
    "POWERGRID.NS",
    "NTPC.NS",
    "IOC.NS",
    "COALINDIA.NS",
    "GAIL.NS",
    "INDUSINDBK.NS",
    "TECHM.NS",
    "BAJAJ-AUTO.NS",
    "BAJAJFINSERV.NS",
    "DIVISLAB.NS",
    "DLF.NS",
    "DRREDDY.NS",
    "EICHERMOT.NS",
    "GRASIM.NS",
    "HCLTECH.NS",
    "HINDALCO.NS",
    "IOC.NS",
    "INFOEDGE.NS",
    "JSWSTEEL.NS",
    "LICI.NS",
    "LTIMINDTREE.NS",
    "M&M.NS",
    "NAUKRI.NS",
    "PNB.NS",
    "PFIZER.NS",      # filler/example; ensure live accuracy
    "SHREECEM.NS",
    "SIEMENS.NS",
    "TATAMOTORS.NS",
    "TATAPOWER.NS",
    "TVSMOTOR.NS",
    "VEDL.NS",
    "ZEEL.NS",
    "^NSEI"
]

with engine.connect() as conn:
    for symbol in symbols:
        conn.execute(text("INSERT INTO stocks (symbol) VALUES (:symbol) ON CONFLICT DO NOTHING"),
                     {"symbol": symbol})
    conn.commit()
