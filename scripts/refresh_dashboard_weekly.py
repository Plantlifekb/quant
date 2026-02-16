import pandas as pd
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------
# Paths (aligned with your verification pipeline)
# ---------------------------------------------------------

BASE = Path(r"C:\Quant")
DATA = BASE / "data"
CANON = DATA / "canonical"
ANALYTICS = DATA / "analytics"
LOGS = BASE / "logs"

INPUT_CSV = CANON / "quant_dashboard_inputs_v2.csv"
OUTPUT_PARQUET = ANALYTICS / "strategy_returns_weekly.parquet"
LOG_FILE = LOGS / "dashboard_weekly_refresh.log"


# ---------------------------------------------------------
# Logging helper
# ---------------------------------------------------------

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line)


# ---------------------------------------------------------
# Main refresh logic
# ---------------------------------------------------------

def main():
    log("START dashboard weekly parquet refresh")

    # --- 1. Validate input exists ---
    if not INPUT_CSV.exists():
        log(f"ERROR: Missing required input file: {INPUT_CSV}")
        return

    # --- 2. Load CSV ---
    try:
        df = pd.read_csv(INPUT_CSV)
    except Exception as e:
        log(f"ERROR: Failed to read CSV: {e}")