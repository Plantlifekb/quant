import pandas as pd
from pathlib import Path

# ---------------------------------------------------------
# INPUT FILES
# ---------------------------------------------------------
PICKS_FILE = Path(r"C:\Quant\data\analytics\weekly_picks_quant_v2.parquet")
PRICES_FILE = Path(r"C:\Quant\data\ingestion\last_month_prices.csv")

# ---------------------------------------------------------
# LOAD WEEKLY PICKS
# ---------------------------------------------------------
print("[INFO] Loading weekly picks...")
picks = pd.read_parquet(PICKS_FILE)
picks = picks.sort_values("week_start")

# Last 4 weeks
last_weeks = picks["week_start"].drop_duplicates().tail(4).tolist()
print(f"[INFO] Last 4 weeks: {last_weeks}")

# Tickers per week
week_to_tickers = {
    wk: picks[picks["week_start"] == wk]["ticker"].drop_duplicates().tolist()
    for wk in last_weeks
}

print("\n[INFO] Tickers per week:")
for wk, tks in week_to_tickers.items():
    print(f"{wk.date()}: {tks}")

# ---------------------------------------------------------
# LOAD PRICES
# ---------------------------------------------------------
print("\n[INFO] Loading extracted prices...")
prices = pd.read_csv(PRICES_FILE)
prices["date"] = pd.to_datetime(prices["date"])

# ---------------------------------------------------------
# FUNCTION: compute weekly return for a set of tickers
# ---------------------------------------------------------
def compute_weekly_return(week_start, tickers):
    # Find the next available trading day after week_start
    start_date = prices[prices["date"] >= week_start]["date"].min()
    end_date = prices[prices["date"] > week_start]["date"].min()

    if pd.isna(start_date) or pd.isna(end_date):
        return None

    # Filter prices
    p0 = prices[(prices["date"] == start_date) & (prices["ticker"].isin(tickers))]
    p1 = prices[(prices["date"] == end_date) & (prices["ticker"].isin(tickers))]

    # Merge to align tickers
    merged = pd.merge(p0, p1, on="ticker", suffixes=("_start", "_end"))

    # Compute returns
    merged["ret"] = merged["adj_close_end"] / merged["adj_close_start"] - 1

    # Equal-weight average
    return merged["ret"].mean()


# ---------------------------------------------------------
# COMPUTE WEEKLY RETURNS
# ---------------------------------------------------------
weekly_returns = []

print("\n[INFO] Weekly returns:")
for wk in last_weeks:
    r = compute_weekly_return(wk, week_to_tickers[wk])
    weekly_returns.append(r)
    print(f"{wk.date()}: {r:.4%}")

# ---------------------------------------------------------
# AGGREGATE
# ---------------------------------------------------------
weekly_avg = sum(weekly_returns) / len(weekly_returns)
monthly = (1 + weekly_avg)**4 - 1
annual = (1 + weekly_avg)**52 - 1

print("\n[RESULTS] Independent Performance Calculation")
print(f"Weekly (4-week avg): {weekly_avg:.4%}")
print(f"Monthly: {monthly:.4%}")
print(f"Annual: {annual:.4%}")