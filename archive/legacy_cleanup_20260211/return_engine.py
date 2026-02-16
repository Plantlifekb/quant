import numpy as np
import pandas as pd


def return_engine(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the governed return suite for each ticker.

    Inputs
    -------
    prices : DataFrame with columns:
        - ticker : str
        - date   : datetime-like
        - close  : float

    Outputs
    -------
    DataFrame enriched with:
        - return          : daily percent return
        - log_return      : log return
        - cum_return      : cumulative return
        - weekly_return   : 5-day percent return
        - monthly_return  : 21-day percent return
        - annual_return   : 252-day percent return
        - vol_20d         : 20-day rolling volatility
    """

    prices = prices.sort_values(["ticker", "date"]).copy()

    prices["return"] = prices.groupby("ticker")["close"].pct_change()

    prices["log_return"] = np.log(
        prices["close"] /
        prices.groupby("ticker")["close"].shift(1)
    )

    prices["cum_return"] = (
        (1 + prices["return"])
        .groupby(prices["ticker"])
        .cumprod()
        - 1
    )

    prices["weekly_return"] = prices.groupby("ticker")["close"].pct_change(5)
    prices["monthly_return"] = prices.groupby("ticker")["close"].pct_change(21)
    prices["annual_return"] = prices.groupby("ticker")["close"].pct_change(252)

    prices["vol_20d"] = (
        prices.groupby("ticker")["return"]
        .rolling(20)
        .std()
        .reset_index(level=0, drop=True)
    )

    return prices