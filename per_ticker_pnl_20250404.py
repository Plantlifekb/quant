# per_ticker_pnl_20250404.py
import pandas as pd
from pathlib import Path
import numpy as np
import sys

DATE = pd.to_datetime("2025-04-04")
OUT_DIR = Path(r"C:\Quant\analysis")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --- Helper to load a CSV if present, else return None ---
def try_load(path):
    p = Path(path)
    if p.exists():
        print(f"Loaded: {p}")
        return pd.read_csv(p, index_col=0, parse_dates=True)
    return None

# --- 1) Try to load a positions snapshot (weights or notional) for the date ---
# Common patterns: daily positions file, parquet, or a single CSV with date index
candidates = [
    r"C:\Quant\data\positions_daily.csv",
    r"C:\Quant\data\positions.csv",
    r"C:\Quant\data\positions.parquet",
    r"C:\Quant\data\holdings_daily.csv",
    r"C:\Quant\data\holdings.csv",
]
positions = None
for c in candidates:
    try:
        df = try_load(c)
        if df is None:
            continue
        # if date is index, take row; else if file contains a 'date' column, filter
        if DATE in df.index:
            positions = df.loc[DATE]
            break
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            row = df[df['date'] == DATE]
            if not row.empty:
                positions = row.iloc[0].drop(labels=['date'], errors='ignore')
                break
        # if file is a single snapshot (columns = tickers), use it as-is
        if df.index.dtype == object and df.shape[0] == 1:
            positions = df.iloc[0]
            break
    except Exception:
        continue

# --- 2) Try to load per-ticker pnl for the date (preferred) ---
pnl_by_ticker = None
candidates_pnl = [
    r"C:\Quant\data\pnl_by_ticker.csv",
    r"C:\Quant\data\pnl_by_ticker.parquet",
    r"C:\Quant\data\pnl_by_ticker_2025-04-04.csv",
]
for c in candidates_pnl:
    try:
        df = try_load(c)
        if df is None:
            continue
        # if df indexed by date and ticker columns, pick the date row
        if DATE in df.index:
            pnl_by_ticker = df.loc[DATE].dropna()
            break
        # if df has columns ['date','ticker','pnl'] convert to series
        if set(['date','ticker','pnl']).issubset(df.columns):
            df['date'] = pd.to_datetime(df['date'])
            s = df[df['date'] == DATE].set_index('ticker')['pnl']
            if not s.empty:
                pnl_by_ticker = s
                break
        # if file is a single column series with tickers as index
        if df.shape[1] == 1 and df.index.dtype == object:
            pnl_by_ticker = df.iloc[:,0].dropna()
            break
    except Exception:
        continue

# --- 3) If pnl_by_ticker not available, try to compute from prices + positions ---
if pnl_by_ticker is None:
    # try to load prices (wide format: index=date, columns=tickers)
    prices_candidates = [
        r"C:\Quant\data\prices.csv",
        r"C:\Quant\data\prices.parquet",
        r"C:\Quant\data\price_history.csv",
    ]
    prices = None
    for c in prices_candidates:
        try:
            df = try_load(c)
            if df is None:
                continue
            prices = df
            break
        except Exception:
            continue

    if prices is not None:
        # ensure DATE and previous business day exist
        if DATE not in prices.index:
            print(f"Price file loaded but {DATE.date()} not in index.")
        else:
            # find previous available date
            prev_idx = prices.index.get_loc(DATE) - 1
            if prev_idx < 0:
                print("No prior price available to compute daily returns.")
            else:
                prev_date = prices.index[prev_idx]
                # compute returns for the date
                returns = (prices.loc[DATE] / prices.loc[prev_date]) - 1
                pnl_by_ticker = returns  # per-unit return; will multiply by position size below
                print(f"Computed per-ticker returns from prices: {prev_date.date()} -> {DATE.date()}")

# --- 4) If still missing positions or pnl_by_ticker, try trades for that date to infer impacts ---
trades = None
if (positions is None or pnl_by_ticker is None):
    trades_candidates = [
        r"C:\Quant\data\trades.csv",
        r"C:\Quant\data\executions.csv",
    ]
    for c in trades_candidates:
        try:
            df = try_load(c)
            if df is None:
                continue
            # expect columns: date, ticker, qty, price, notional
            df['date'] = pd.to_datetime(df['date'])
            trades = df[df['date'] == DATE]
            if not trades.empty:
                print(f"Loaded trades for {DATE.date()} from {c}")
                break
        except Exception:
            continue

# --- Compute per-position impact if we have enough data ---
per_pos_impact = None

# Case A: have positions snapshot and pnl_by_ticker (returns or pnl)
if positions is not None and pnl_by_ticker is not None:
    # align tickers
    pos = positions.dropna()
    pnl_series = pnl_by_ticker.dropna()
    # if positions are weights (sum ~1) treat pnl_series as returns; else treat positions as notional
    pos_sum = pos.abs().sum()
    if 0.9 <= pos_sum <= 1.1:
        # positions are weights -> per-position impact = weight * return
        per_pos_impact = (pos.reindex(pnl_series.index).fillna(0) * pnl_series).dropna()
    else:
        # positions are notional or shares -> if pnl_series are returns, multiply; if pnl_series are pnl per unit, multiply directly
        per_pos_impact = (pos.reindex(pnl_series.index).fillna(0) * pnl_series).dropna()

# Case B: have trades only -> aggregate notional by ticker as proxy for impact
if per_pos_impact is None and trades is not None and not trades.empty:
    # compute net notional traded per ticker as a rough proxy (signed)
    if 'notional' in trades.columns:
        per_pos_impact = trades.groupby('ticker')['notional'].sum().sort_values()
    else:
        # compute notional = qty * price if available
        if set(['qty','price']).issubset(trades.columns):
            trades['notional'] = trades['qty'] * trades['price']
            per_pos_impact = trades.groupby('ticker')['notional'].sum().sort_values()
        else:
            per_pos_impact = None

# Final checks and output
if per_pos_impact is None:
    print("\nUnable to compute per-position PnL automatically.")
    print("What I looked for (in order):")
    print("- positions snapshot files in C:\\Quant\\data (CSV/parquet)")
    print("- pnl_by_ticker files or a prices history to compute returns")
    print("- trades/executions for the date")
    print("\nIf you have any of these, place them in C:\\Quant\\data with one of the filenames listed in the script,")
    print("or load them into the current Python session as variables named `positions` and `pnl_by_ticker` and re-run.")
    sys.exit(1)

# Save and print top contributors
out_file = OUT_DIR / f"per_ticker_pnl_{DATE.date()}.csv"
per_pos_impact = per_pos_impact.sort_values()
per_pos_impact.to_csv(out_file, header=["impact"])
print(f"\nWrote per-ticker impacts to: {out_file}\n")
print("Top negative contributors:")
print(per_pos_impact.head(20))
print("\nTop positive contributors:")
print(per_pos_impact.tail(20))