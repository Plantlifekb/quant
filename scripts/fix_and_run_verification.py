import os, sys, json
import pandas as pd
from pathlib import Path
ROOT = Path(r"C:\Quant")
REALIZED = ROOT / "analysis" / "realized_weekly_from_ingest.csv"
INGEST = ROOT / "data" / "ingestion" / "ingestion_5years.csv"
RUNNER = ROOT / "scripts" / "run_verification_using_realized.py"

def ensure_realized():
    if not REALIZED.exists():
        # rebuild from ingestion
        if not INGEST.exists():
            print(json.dumps({"status":"failed","issues":["ingestion file missing"], "message":""}))
            sys.exit(1)
        df = pd.read_csv(INGEST, low_memory=False)
        df.columns = [c.strip().lower() for c in df.columns]
        date_col = next((c for c in df.columns if c in ("date","run_date","trade_date","timestamp")), None)
        ticker_col = next((c for c in df.columns if c in ("ticker","symbol","sid")), None)
        price_col = 'adj_close' if 'adj_close' in df.columns and df['adj_close'].notna().any() else ('close' if 'close' in df.columns and df['close'].notna().any() else None)
        if date_col is None or ticker_col is None or price_col is None:
            print(json.dumps({"status":"failed","issues":["missing required columns in ingestion"], "message":""}))
            sys.exit(1)
        df = df[[date_col, ticker_col, price_col]].dropna(subset=[date_col, ticker_col])
        df = df.rename(columns={date_col:"date", ticker_col:"ticker", price_col:"close"})
        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
        df = df.dropna(subset=["date"])
        df["ticker"] = df["ticker"].astype(str)
        frames = []
        for t, g in df.groupby("ticker"):
            g = g.sort_values("date").set_index("date")
            first = g["close"].resample("W-MON").first()
            last = g["close"].resample("W-MON").last()
            w = pd.DataFrame({"close_start": first, "close_end": last}).dropna()
            if w.empty:
                continue
            w = w.reset_index().rename(columns={"index":"week"})
            w["realized_return"] = w["close_end"] / w["close_start"] - 1.0
            w["ticker"] = t
            frames.append(w[["week","ticker","realized_return"]])
        if not frames:
            print(json.dumps({"status":"failed","issues":["No weekly returns computed from ingestion"], "message":""}))
            sys.exit(1)
        realized = pd.concat(frames, ignore_index=True)
        realized["week"] = pd.to_datetime(realized["week"], errors="coerce").dt.normalize()
        REALIZED.parent.mkdir(parents=True, exist_ok=True)
        realized.to_csv(REALIZED, index=False)
        return realized
    # file exists: load and ensure realized_return
    r = pd.read_csv(REALIZED)
    if "realized_return" in r.columns:
        return r
    # try compute from close_end/close_start if present
    if "close_end" in r.columns and "close_start" in r.columns:
        r["realized_return"] = r["close_end"] / r["close_start"] - 1.0
        r.to_csv(REALIZED, index=False)
        return r
    # fallback: rebuild from ingestion (guaranteed)
    if INGEST.exists():
        return ensure_realized()  # will rebuild
    print(json.dumps({"status":"failed","issues":["realized missing realized_return and cannot be computed"], "message":""}))
    sys.exit(1)

# ensure realized file is present and correct
realized = ensure_realized()

# now run the verification runner script that imports regen_and_verify functions
MODULE = ROOT / "scripts" / "regen_and_verify.py"
if not MODULE.exists():
    print(json.dumps({"status":"failed","issues":["regen_and_verify.py missing"], "message":""}))
    sys.exit(1)

import importlib.util
spec = importlib.util.spec_from_file_location("regen_mod", str(MODULE))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

# normalize week column
if "week" not in realized.columns and "date" in realized.columns:
    realized["week"] = pd.to_datetime(realized["date"], errors="coerce").dt.normalize()
elif "week" in realized.columns:
    realized["week"] = pd.to_datetime(realized["week"], errors="coerce").dt.normalize()
else:
    print(json.dumps({"status":"failed","issues":["realized missing week/date"], "message":""}))
    sys.exit(1)

# ensure realized_return exists
if "realized_return" not in realized.columns:
    print(json.dumps({"status":"failed","issues":["realized_return missing after rebuild"], "message":""}))
    sys.exit(1)

# run verification logic using functions from regen module
artifacts = []
runs = []
issues = []
for strat in ("longonly","longshort"):
    pick_path = mod.find_first_existing(mod.PICK_CANDIDATES[strat])
    if not pick_path:
        runs.append({"strategy":strat,"status":"no_picks"})
        issues.append(f"No pick file for {strat}")
        continue
    picks = mod.load_picks(pick_path)
    if picks is None or picks.empty:
        runs.append({"strategy":strat,"status":"no_picks_or_empty"})
        issues.append(f"Picks empty for {strat}")
        continue
    picks_matched = mod.safe_match(picks, realized)
    res = mod.compute_and_save(strat, picks_matched)
    runs.append(res)
    artifacts.extend([mod.OUT.get("regen_"+strat), mod.OUT.get("summary_"+strat), mod.OUT.get("diag_"+strat), mod.OUT.get("png_"+strat)])

out = {"status":"success","artifacts":[str(a) for a in artifacts if a],"runs":runs,"issues":issues,"message":"verification run complete (forced realized)"} 
print(json.dumps(out))
