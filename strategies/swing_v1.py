import pandas as pd
import pandas_ta as ta

def generate_signals(df, nifty_df):

    df = df.copy()
    nifty_df = nifty_df.copy()

    # ==========================
    # Stock Indicators
    # ==========================

    df["ema_200"] = df["close"].ewm(span=200, adjust=False).mean()
    df["ema_20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["vol_ma_20"] = df["volume"].rolling(20).mean()

    df["hh_20"] = df["high"].rolling(20).max().shift(1)
    df["ll_10"] = df["low"].rolling(10).min().shift(1)

    # ==========================
    # NIFTY Indicator
    # ==========================

    nifty_df["ema_200"] = nifty_df["close"].ewm(span=200, adjust=False).mean()

    df = df.merge(
        nifty_df[["close", "ema_200"]],
        left_index=True,
        right_index=True,
        how="left",
        suffixes=("", "_nifty")
    )

    # ==========================
    # Remove Warmup Rows
    # ==========================

    df = df[
        df["ema_200"].notna() &
        df["ema_20"].notna() &
        df["vol_ma_20"].notna() &
        df["hh_20"].notna() &
        df["ll_10"].notna() &
        df["ema_200_nifty"].notna()
    ].copy()

    # ==========================
    # Entry Conditions
    # ==========================

    trend_condition = df["close"] > df["ema_200"]
    # breakout_condition = df["close"] > df["hh_20"]
    breakout_condition = df["close"] > df["hh_20"] * 1.005
    volume_condition = df["volume"] > 1.5 * df["vol_ma_20"]

    market_condition = df["close_nifty"] > df["ema_200_nifty"]

    df["entry"] = (
        trend_condition &
        breakout_condition &
        volume_condition &
        market_condition 
    )

    # ==========================
    # Stop & Risk
    # ==========================
    df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=14)

    # df["stop"] = df["ll_10"]
    df["stop"] = df["close"] - (2 * df["atr"])

    df["risk"] = df["close"] - df["stop"]

    df.loc[df["risk"] <= 0, "entry"] = False

    # ==========================
    # Exit
    # ==========================

    # df["exit"] = (
    #     (df["close"] < df["ll_10"]) |
    #     (df["close"] < df["ema_20"])
    # )
    df["exit"] = df["close"] < df["ll_10"]

    return df
