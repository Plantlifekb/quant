# import_tickers.py
import os
import pandas as pd
from sqlalchemy import create_engine

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///C:/Quant/data/quant_dev.sqlite")
engine = create_engine(DATABASE_URL)

csv_path = os.path.join("quant", "config", "ticker_reference.csv")
df = pd.read_csv(csv_path)

mapping = {"ticker": "ticker", "company": "company_name", "sector": "market_sector", "avg_daily_volume": "avg_daily_volume"}
df = df.rename(columns={k: v for k, v in mapping.items() if k in df.columns})
cols = [c for c in ["ticker", "company_name", "market_sector", "avg_daily_volume"] if c in df.columns]
df = df[cols]

df.to_sql("tickers", engine, if_exists="replace", index=False)
print(f"Imported {len(df)} tickers into table 'tickers' using {DATABASE_URL}")