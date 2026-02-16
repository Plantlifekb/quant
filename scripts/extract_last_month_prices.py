import pandas as pd
from pathlib import Path

# ---------------------------------------------------------
# INPUT FILES
# ---------------------------------------------------------
PICKS_FILE = Path(r"C:\Quant\data\analytics\weekly_picks_quant_v2.parquet")
INGEST_FILE = Path(r"C:\Quant\data\ingestion\ingestion_5years.csv")

# ---------------------------------------------------------
# OUTPUT FILE
# ---------------------------------------------------------
OUTFILE = Path(r"C:\Quant\data\ingestion\last_month_prices.csv")

# ---------------------------------------------------------
# LOAD WEEKLY PICKS
# ---------------------------------------------------------
print("[INFO] Loading weekly picks...")
df = pd.read_parquet(PICKS_FILE)
df = df.sort_values("week_start")

# ---------------------------------------------------------
# GET LAST 4 UNIQUE WEEKS
# ---------------------------------------------------------
last_weeks = df["week_start"].drop_duplicates().tail(4).tolist()
print(f"[INFO] Last 4 weeks: {last_weeks}")

# ---------------------------------------------------------
# GET TICKERS FOR THOSE WEEKS
# ---------------------------------------------------------
tickers = (
    df[df["week_start"].isin(last_weeks)]["ticker"]
    .drop_duplicates()
    .tolist()
)

print(f"[INFO] Tickers in last 4 weeks ({len(tickers)}): {tickers}")

# ---------------------------------------------------------
# LOAD INGESTION CSV (FILTERED)
# ---------------------------------------------------------
print("[INFO] Loading ingestion CSV (this may take a moment)...")
usecols = ["date", "ticker", "adj_close"]
ing = pd.read_csv(INGEST_FILE, usecols=usecols)

# Convert date to datetime
ing["date"] = pd.to_datetime(ing["date"], dayfirst=True)

# ---------------------------------------------------------
# FILTER FOR ONLY THE TICKERS WE NEED
# ---------------------------------------------------------
filtered = ing[ing["ticker"].isin(tickers)].copy()

print(f"[INFO] Filtered ingestion rows: {filtered.shape[0]}")

# ---------------------------------------------------------
# FILTER FOR ONLY THE LAST ~40 DAYS
# ---------------------------------------------------------
max_date = filtered["date"].max()
min_date = max_date - pd.Timedelta(days=40)

filtered = filtered[(filtered["date"] >= min_date) & (filtered["date"] <= max_date)]

print(f"[INFO] Final rows after date filter: {filtered.shape[0]}")
print(f"[INFO] Date range: {filtered['date'].min()} → {filtered['date'].max()}")

# ---------------------------------------------------------
# SAVE OUTPUT
# ---------------------------------------------------------
filtered.to_csv(OUTFILE, index=False)
print(f"[INFO] Written to: {OUTFILE}")