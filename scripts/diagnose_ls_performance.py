import pandas as pd
from pathlib import Path

# ---------------------------------------------------------
# INPUT FILES
# ---------------------------------------------------------
PICKS_FILE = Path(r"C:\Quant\data\analytics\weekly_picks_quant_v2.parquet")
PRICES_FILE = Path(r"C:\Quant\data\ingestion\ingestion_5years.csv")
WEEKLY_FILE = Path(r"C:\Quant\data\analytics\strategy_returns_weekly.parquet")

# ---------------------------------------------------------
# LOAD WEEKLY PICKS
# ---------------------------------------------------------
print("[INFO] Loading weekly picks...")
picks = pd.read_parquet(PICKS_FILE)
picks["week_start"] = pd.to_datetime(picks["week_start"])
picks = picks.sort_values("week_start")

# Last 4 weeks
last_weeks = picks["week_start"].drop_duplicates().tail(4).tolist()
print(f"[INFO] Last 4 weeks: {last_weeks}")

# ---------------------------------------------------------
# IDENTIFY LONG AND SHORT LEGS
# ---------------------------------------------------------
# Assumption:
#   pick_rank 1–10 = LONG
#   pick_rank 11–20 = SHORT
# Adjust if your ranking logic differs.

def get_legs(week):
    df = picks[picks["week_start"] == week]
    long_leg = df[df["pick_rank"] <= 10]["ticker"].unique().tolist()
    short_leg = df[df["pick_rank"] > 10]["ticker"].unique().tolist()
    return long_leg, short_leg

week_legs = {wk: get_legs(wk) for wk in last_weeks}

print("\n[INFO] Long/Short legs per week:")
for wk, (L, S) in week_legs.items():
    print(f"{wk.date()}  LONG={L}   SHORT={S}")

# ---------------------------------------------------------
# LOAD PRICES
# ---------------------------------------------------------
print("\n[INFO] Loading ingestion prices...")
prices = pd.read_csv(PRICES_FILE)
prices["date"] = pd.to_datetime(prices["date"])

# ---------------------------------------------------------
# FUNCTION: compute weekly return for a leg
# ---------------------------------------------------------
def compute_leg_return(week_start, tickers, short=False):
    if len(tickers) == 0:
        return 0.0

    start_date = prices[prices["date"] >= week_start]["date"].min()
    end_date = prices[prices["date"] > week_start]["date"].min()

    p0 = prices[(prices["date"] == start_date) & (prices["ticker"].isin(tickers))]
    p1 = prices[(prices["date"] == end_date) & (prices["ticker"].isin(tickers))]

    merged = pd.merge(p0, p1, on="ticker", suffixes=("_start", "_end"))
    merged["ret"] = merged["adj_close_end"] / merged["adj_close_start"] - 1

    if short:
        merged["ret"] = -merged["ret"]

    return merged["ret"].mean()

# ---------------------------------------------------------
# COMPUTE INDEPENDENT RETURNS
# ---------------------------------------------------------
results = []

print("\n[INFO] Computing independent weekly returns...")
for wk in last_weeks:
    long_leg, short_leg = week_legs[wk]

    r_long = compute_leg_return(wk, long_leg, short=False)
    r_short = compute_leg_return(wk, short_leg, short=True)
    r_ls = r_long + r_short

    results.append((wk, r_long, r_short, r_ls))

print("\nIndependent L/S Breakdown:")
print("Week        Long       Short      L/S")
for wk, rl, rs, rls in results:
    print(f"{wk.date()}   {rl: .4%}   {rs: .4%}   {rls: .4%}")

# ---------------------------------------------------------
# LOAD DASHBOARD L/S RETURNS
# ---------------------------------------------------------
print("\n[INFO] Loading dashboard weekly file...")
weekly = pd.read_parquet(WEEKLY_FILE)
weekly["date"] = pd.to_datetime(weekly["date"])

ls_df = weekly[weekly["strategy"] == "LONG_SHORT"].sort_values("date")
ls_last4 = ls_df.tail(4)["weekly_return"].tolist()

print("\nDashboard L/S weekly returns:")
for d, r in zip(ls_df.tail(4)["date"], ls_last4):
    print(f"{d.date()}   {r:.4%}")

# ---------------------------------------------------------
# COMPARE
# ---------------------------------------------------------
print("\nComparison (Independent vs Dashboard):")
for (wk, rl, rs, rls), dash in zip(results, ls_last4):
    print(f"{wk.date()}   Independent L/S={rls:.4%}   Dashboard L/S={dash:.4%}")

print("\n[INFO] Complete.")