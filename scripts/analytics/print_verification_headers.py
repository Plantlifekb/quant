#!/usr/bin/env python3
# print_verification_headers.py
from pathlib import Path
import pandas as pd

files = [
  Path("outputs/verification/predicted_vs_picks_weekly_longonly.csv"),
  Path("outputs/verification/realized_vs_picks_weekly_longonly.csv"),
  Path("outputs/verification/predicted_vs_picks_weekly_longshort.csv"),
  Path("outputs/verification/realized_vs_picks_weekly_longshort.csv"),
  Path("outputs/verification/weekly_portfolio_predicted_vs_realized.csv"),
]

for p in files:
    print("----", p.name, "----")
    if p.exists():
        df = pd.read_csv(p, nrows=0)
        print(list(df.columns))
    else:
        print("MISSING")