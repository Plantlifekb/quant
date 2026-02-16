import pandas as pd
from pathlib import Path

# ---------------------------------------------------------
# INPUT FILES
# ---------------------------------------------------------
PICKS_FILE = Path(r"C:\Quant\data\analytics\weekly_picks_quant_v2.parquet")
PRICES_FILE = Path(r"C:\Quant\data\ingestion\ingestion_5years.csv")

# ---------------------------------------------------------
# OUTPUT FILE
# ---------------------------------------------------------
OUTPUT = Path(r"C:\Quant\data\analytics\strategy_returns_weekly.parquet")

# ---------------------------------------------------------
# LOAD WEEKLY PICKS
# ---------------------------------------------------------
print("[INFO] Loading weekly picks...")
picks = pd.read_parquet(PICKS_FILE)
picks["week_start"] = pd.to_datetime(picks["week_start"])

# Ensure ranks exist
if "pick_rank" not in picks.columns:
    raise SystemExit("[FATAL] Picks file must contain 'pick_rank'.")

# Sort by week + rank
picks = picks.sort_values(["week_start", "pick_rank"])

weeks = picks["week_start"].drop_duplicates().tolist()

# ---------------------------------------------------------
# DEFINE LONG AND SHORT LEGS USING RANKS
# ---------------------------------------------------------
N = 10  # number of longs and shorts

def get_legs(week):
    df = picks[picks["week_start"] == week]

    # Longs = top N ranks
    long_leg = df[df["pick_rank"] <= N]["ticker"].unique().tolist()

    # Shorts = bottom N ranks
    max_rank = df["pick_rank"].max()
    short_leg = df[df["pick_rank"] > (max_rank - N)]["ticker"].unique().tolist()

    return long_leg, short_leg

week_legs = {wk: get_legs(wk) for wk in weeks}

print("[INFO] Sample week legs:")
for wk in weeks[-4:]:
    L, S = week_legs[wk]
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
# BUILD WEEKLY RETURNS FOR BOTH STRATEGIES
# ---------------------------------------------------------
records = []

print("\n[INFO] Computing weekly returns for all weeks...")

for wk in weeks:
    long_leg, short_leg = week_legs[wk]

    r_long = compute_leg_return(wk, long_leg, short=False)
    r_short = compute_leg_return(wk, short_leg, short=True)
    r_ls = r_long + r_short

    records.append({
        "strategy": "LONG_ONLY",
        "date": wk,
        "weekly_return": r_long
    })
    records.append({
        "strategy": "LONG_SHORT",
        "date": wk,
        "weekly_return": r_ls
    })

weekly = pd.DataFrame(records).sort_values(["strategy", "date"])

# ---------------------------------------------------------
# COMPUTE CUMULATIVE RETURNS
# ---------------------------------------------------------
weekly["cum_return"] = (
    weekly.groupby("strategy")["weekly_return"]
          .apply(lambda s: (1 + s).cumprod())
          .reset_index(level=0, drop=True)
)

print("\n[INFO] Final weekly file preview:")
print(weekly.tail())

print("\n[INFO] Writing:", OUTPUT)
weekly.to_parquet(OUTPUT, index=False)

print("[INFO] Done. Symmetric L/S and Long‑Only weekly returns built successfully.")