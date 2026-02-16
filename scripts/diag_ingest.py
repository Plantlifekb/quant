import pandas as pd
p = r"C:\Quant\data\ingestion\ingestion_5years.csv"
df = pd.read_csv(p, nrows=5000)
df.columns = [c.strip().lower() for c in df.columns]
print("columns:", df.columns.tolist())
date_col = next((c for c in df.columns if c in ("date","run_date","trade_date","timestamp")), None)
print("detected date column:", date_col)
if date_col:
    parsed = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True)
    print("date parse non-null:", parsed.notna().sum(), "/", len(df))
print("adj_close present:", "adj_close" in df.columns, "close present:", "close" in df.columns)
if "adj_close" in df.columns:
    print("adj_close non-null:", df["adj_close"].notna().sum(), "/", len(df))
if "close" in df.columns:
    print("close non-null:", df["close"].notna().sum(), "/", len(df))
if "ticker" in df.columns:
    print("sample tickers:", df["ticker"].dropna().unique()[:10].tolist())
# sample ticker weekly test
if "ticker" in df.columns and date_col:
    t = df["ticker"].dropna().unique()[0]
    print("testing weekly for ticker:", t)
    sub = df[df["ticker"]==t].copy()
    sub["date"] = pd.to_datetime(sub[date_col], errors="coerce", dayfirst=True)
    sub = sub.dropna(subset=["date"])
    sub = sub.sort_values("date").set_index("date")
    price = sub["adj_close"] if "adj_close" in sub.columns else sub["close"]
    first = price.resample("W-MON").first()
    last = price.resample("W-MON").last()
    w = pd.DataFrame({"first": first, "last": last}).dropna()
    print("weekly rows for sample ticker:", len(w))
    print("weekly sample:", w.head().to_dict(orient="records"))
