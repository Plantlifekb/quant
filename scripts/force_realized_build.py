# force_realized_build.py
import os, pandas as pd
ROOT = r"C:\Quant"
INGEST = os.path.join(ROOT, "data", "ingestion", "ingestion_5years.csv")
OUT = os.path.join(ROOT, "analysis", "realized_weekly_from_ingest.csv")
df = pd.read_csv(INGEST, low_memory=False)
df.columns = [c.strip().lower() for c in df.columns]
date_col = next((c for c in df.columns if c in ("date","run_date","trade_date","timestamp")), None)
ticker_col = next((c for c in df.columns if c in ("ticker","symbol","sid")), None)
price_col = 'adj_close' if 'adj_close' in df.columns and df['adj_close'].notna().any() else ('close' if 'close' in df.columns and df['close'].notna().any() else None)
if date_col is None or ticker_col is None or price_col is None:
    raise SystemExit("missing required columns")
df = df[[date_col, ticker_col, price_col]].dropna(subset=[date_col, ticker_col])
df = df.rename(columns={date_col: "date", ticker_col: "ticker", price_col: "close"})
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
    w["realized_return"] = w["close_end"] / w["close_start"] - 1.0
    w = w.reset_index().rename(columns={"index": "week"})
    w["ticker"] = t
    frames.append(w[["week","ticker","realized_return"]])
if not frames:
    raise SystemExit("No weekly returns computed")
realized = pd.concat(frames, ignore_index=True)
realized["week"] = pd.to_datetime(realized["week"]).dt.normalize()
realized.to_csv(OUT, index=False)
print("wrote", OUT, "rows:", len(realized))
