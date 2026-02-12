import pandas as pd

def backtest(df, initial_capital=1000000, risk_per_trade=0.01):

    capital = initial_capital
    trades = []
    lossers = 0
    winners = 0
    in_position = False

    for i in range(1, len(df)):

        if pd.isna(df["risk"].iloc[i]):
            continue

        if df["risk"].iloc[i] <= 0:
            continue


        if not in_position and df["entry"].iloc[i]:
            
            entry_price = df["close"].iloc[i]
            stop = df["stop"].iloc[i]
            target = df["target"].iloc[i]

            risk_amount = capital * risk_per_trade
            qty = risk_amount / (entry_price - stop)

            in_position = True
            entry_index = i

        if in_position:

            low = df["low"].iloc[i]
            high = df["high"].iloc[i]

            # Stoploss hit
            if low <= stop:
                pnl = (stop - entry_price) * qty
                capital += pnl

                trades.append(pnl)
                lossers = lossers + 1
                in_position = False

            # Target hit
            elif high >= target:
                pnl = (target - entry_price) * qty
                capital += pnl

                trades.append(pnl)
                winners = winners + 1
                in_position = False

    return capital, trades, winners, lossers
