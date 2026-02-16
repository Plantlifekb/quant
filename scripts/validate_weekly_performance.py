import pandas as pd
from pathlib import Path

PICKS = Path(r"C:\Quant\data\analytics\weekly_picks_quant_v2.parquet")
PRICES = Path(r"C:\Quant\data\ingestion\last_month_prices.csv")

picks = pd.read_parquet(PICKS).sort_values("week_start")
last_weeks = picks["week_start"].drop_duplicates().tail(4).tolist()

week_to_tickers = {
    wk: picks[picks["week_start"] == wk]["ticker"].drop_duplicates().tolist()
    for wk in last_weeks
}

prices = pd.read_csv(PRICES)
prices["date"] = pd.to_datetime(prices["date"])

def compute_weekly_return(week_start, tickers):
    start_date = prices[prices["date"] >= week_start]["date"].min()
    end_date = prices[prices["date"] > week_start]["date"].min()

    p0 = prices[(prices["date"] == start_date) & (prices["ticker"].isin(tickers))]
    p1 = prices[(prices["date"] == end_date) & (prices["ticker"].isin(tickers))]

    merged = pd.merge(p0, p1, on="ticker", suffixes=("_start", "_end"))
    merged["ret"] = merged["adj_close_end"] / merged["adj_close_start"] - 1
    return merged["ret"].mean()

weekly_returns = [compute_weekly_return(wk, week_to_tickers[wk]) for wk in last_weeks]

weekly_avg = sum(weekly_returns) / len(weekly_returns)
monthly = (1 + weekly_avg)**4 - 1
annual = (1 + weekly_avg)**52 - 1

print("\nIndependent Validation")
print("----------------------")
for wk, r in zip(last_weeks, weekly_returns):
    print(f"{wk.date()}: {r:.4%}")

print("\nWeekly avg:", f"{weekly_avg:.4%}")
print("Monthly:", f"{monthly:.4%}")
print("Annual:", f"{annual:.4%}")