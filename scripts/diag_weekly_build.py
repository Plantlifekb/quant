# C:\Quant\scripts\diag_weekly_build.py
import sys, traceback
import pandas as pd
from pathlib import Path

p = Path(r"C:\Quant\data\ingestion\ingestion_5years.csv")
print("file exists:", p.exists())
try:
    df = pd.read_csv(p, low_memory=False)
    df.columns = [c.strip().lower() for c in df.columns]
    print("columns:", df.columns.tolist())
    # detect columns
    date_col = next((c for c in df.columns if c in ("date","run_date","trade_date","timestamp")), None)
    ticker_col = next((c for c in df.columns if c in ("ticker","symbol","sid")), None)
    price_candidates = ["adj_close","adjclose","close","close_price","price","last"]
    price_col = next((c for c in price_candidates if c in df.columns and df[c].notna().any()), None)
    print("detected date_col:", date_col)
    print("detected ticker_col:", ticker_col)
    print("detected price_col:", price_col)
    # quick parse check
    if date_col:
        parsed = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True)
        print("date parse non-null:", parsed.notna().sum(), "/", len(df))
    # prepare minimal frame
    df = df[[date_col, ticker_col, price_col]].dropna(subset=[date_col, ticker_col])
    df = df.rename(columns={date_col:"date", ticker_col:"ticker", price_col:"close"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["date"])
    df["ticker"] = df["ticker"].astype(str)
    tickers = df["ticker"].unique().tolist()
    print("unique tickers count:", len(tickers))
    # iterate and collect stats
    total_weekly_rows = 0
    zero_week_tickers = []
    sample_rows = []
    for i, t in enumerate(tickers):
        sub = df[df["ticker"]==t].sort_values("date").set_index("date")
        try:
            first = sub["close"].resample("W-MON").first()
            last = sub["close"].resample("W-MON").last()
            w = pd.DataFrame({"first": first, "last": last}).dropna()
            n = len(w)
            total_weekly_rows += n
            if n == 0:
                zero_week_tickers.append(t)
            if len(sample_rows) < 5 and n>0:
                # capture first non-empty ticker sample
                sample_rows.append((t, w.head(3).to_dict(orient="records")))
        except Exception as e:
            print("exception for ticker", t, ":", e)
        if i % 200 == 0:
            print(f"processed {i}/{len(tickers)} tickers")
    print("total weekly rows across tickers:", total_weekly_rows)
    print("tickers with zero weekly rows (sample 20):", zero_week_tickers[:20])
    print("sample weekly rows for first non-empty tickers:")
    for t, rows in sample_rows:
        print("ticker:", t, "rows:", rows)
except Exception:
    print("exception building weekly returns")
    traceback.print_exc()
    sys.exit(2)