import pandas as pd
from pathlib import Path

prices_path = Path(r"C:\Quant\data\analytics\quant_prices_v1.csv")
out_path = Path(r"C:\Quant\data\analytics\price_returns.parquet")

df = pd.read_csv(prices_path)
cols = {c.lower(): c for c in df.columns}
if 'close' in cols:
    price_col = cols['close']
elif 'price' in cols:
    price_col = cols['price']
else:
    raise SystemExit("prices file missing 'close' or 'price' column: " + ", ".join(df.columns))

df['date'] = pd.to_datetime(df['date'], errors='coerce')
df = df.sort_values(['ticker', 'date'])
df = df[['date', 'ticker', price_col]].rename(columns={price_col: 'close'})
df['close'] = pd.to_numeric(df['close'], errors='coerce')
df = df.dropna(subset=['date', 'ticker', 'close'])
df['return'] = df.groupby('ticker')['close'].pct_change().fillna(0.0)
out = df[['date', 'ticker', 'return']].copy()
out['date'] = out['date'].dt.date
out.to_parquet(out_path, index=False)
print("WROTE", out_path, "rows", len(out))