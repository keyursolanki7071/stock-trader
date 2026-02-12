import pandas_ta as ta

def add_indicators(df):
    df["ema_50"] = ta.ema(df["close"], length=50)
    df["ema_20"] = ta.ema(df["close"], length=20)
    df["ema_200"] = ta.ema(df["close"], length=200)
    df["rsi"] = ta.rsi(df["close"], length=14)
    df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=14)
    df["vol_ma"] = df["volume"].rolling(20).mean()
    df["high_20"] = df["high"].rolling(20).max()
    return df