import pandas as pd
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------

DATA_DIR = Path(r"C:\Quant\data\analytics")
OUTPUT = DATA_DIR / "strategy_returns_weekly.parquet"
LOG = DATA_DIR / "refresh_log.txt"

# Your model output (daily returns or raw signals)
MODEL_OUTPUT = Path(r"C:\Quant\data\model_output\strategy_returns_daily.parquet")

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG, "a") as f:
        f.write(f"[{timestamp}] {msg}\n")
    print(msg)

# ---------------------------------------------------------
# Load daily returns
# ---------------------------------------------------------

def load_daily():
    if not MODEL_OUTPUT.exists():
        raise FileNotFoundError(f"Missing model output: {MODEL_OUTPUT}")
    df = pd.read_parquet(MODEL_OUTPUT)
    df["date"] = pd.to_datetime(df["date"])
    return df

# ---------------------------------------------------------
# Compute weekly returns
# ---------------------------------------------------------

def compute_weekly(df):
    df = df.sort_values(["strategy", "date"])
    df["week_start"] = df["date"] - df["date"].dt.weekday * pd.Timedelta(days=1)
    weekly = (
        df.groupby(["strategy", "week_start"])["daily_return"]
        .apply(lambda x: (1 + x).prod() - 1)
        .reset_index(name="weekly_return")
    )
    weekly["cum_return"] = (
        weekly.groupby("strategy")["weekly_return"]
        .apply(lambda x: (1 + x).cumprod())
        .reset_index(drop=True)
    )
    return weekly

# ---------------------------------------------------------
# Compare old vs new
# ---------------------------------------------------------

def compare(old, new):
    if old is None:
        return f"Initial load: {len(new)} rows"

    old_rows = len(old)
    new_rows = len(new)
    delta = new_rows - old_rows

    if delta == 0:
        return f"No new rows ({new_rows} total)"
    elif delta > 0:
        return f"Added {delta} new rows ({new_rows} total)"
    else:
        return f"WARNING: row count decreased ({old_rows} → {new_rows})"

# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():
    log("Starting daily refresh")

    df_daily = load_daily()
    df_weekly = compute_weekly(df_daily)

    old = None
    if OUTPUT.exists():
        old = pd.read_parquet(OUTPUT)

    summary = compare(old, df_weekly)
    log(summary)

    df_weekly.to_parquet(OUTPUT, index=False)
    log(f"Updated: {OUTPUT}")

    log("Refresh complete\n")

if __name__ == "__main__":
    main()