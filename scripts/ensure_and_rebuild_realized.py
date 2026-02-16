import sys, json
from pathlib import Path
import pandas as pd

ROOT = Path(r"C:\Quant")
REALIZED = ROOT / "analysis" / "realized_weekly_from_ingest.csv"
INGEST = ROOT / "data" / "ingestion" / "ingestion_5years.csv"

if not REALIZED.exists():
    print(json.dumps({"status":"failed","issue":"realized_missing","path":str(REALIZED)}))
    sys.exit(1)

r = pd.read_csv(REALIZED)
cols = r.columns.tolist()
sample = r.head(3).to_dict(orient="records")
out = {"status":"checking","columns":cols,"sample":sample}

if "realized_return" in r.columns:
    out["status"] = "already_ok"
    print(json.dumps(out))
    sys.exit(0)

# try compute from close_end/close_start
if "close_end" in r.columns and "close_start" in r.columns:
    r["realized_return"] = r["close_end"] / r["close_start"] - 1.0
    r.to_csv(REALIZED, index=False)
    out["status"] = "created_from_closes"
    out["created_from"] = ["close_start","close_end"]
    out["rows"] = len(r)
    print(json.dumps(out))
    sys.exit(0)

# fallback: rebuild weekly realized from ingestion (guaranteed)
if not INGEST.exists():
    out["status"] = "failed"
    out["issue"] = "ingest_missing_for_rebuild"
    print(json.dumps(out))
    sys.exit(2)

df = pd.read_csv(INGEST, low_memory=False)
df.columns = [c.strip().lower() for c in df.columns]
date_col = next((c for c in df.columns if c in ("date","run_date","trade_date","timestamp")), None)
ticker_col = next((c for c in df.columns if c in ("ticker","symbol","sid")), None)
price_col = 'adj_close' if 'adj_close' in df.columns and df['adj_close'].notna().any() else ('close' if 'close' in df.columns and df['close'].notna().any() else None)
if date_col is None or ticker_col is None or price_col is None:
    out["status"] = "failed"
    out["issue"] = "missing_columns_in_ingest"
    out["ingest_columns"] = df.columns.tolist()
    print(json.dumps(out))
    sys.exit(3)

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
    out["status"] = "failed"
    out["issue"] = "no_weekly_returns_from_ingest"
    print(json.dumps(out))
    sys.exit(4)

realized = pd.concat(frames, ignore_index=True)
realized["week"] = pd.to_datetime(realized["week"], errors="coerce").dt.normalize()
realized.to_csv(REALIZED, index=False)
out["status"] = "rebuilt_from_ingest"
out["rows"] = len(realized)
print(json.dumps(out))
