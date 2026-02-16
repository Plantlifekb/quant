# C:\Quant\reconcile_trades_vs_mark.py
"""
Reconcile trades vs per-ticker mark impacts for 2025-04-04.
- Searches common locations for trades files and per_ticker_pnl CSV.
- Loads trades if found, filters by date and tickers (top contributors by default).
- Computes simple realized notional and realized PnL proxy and compares to mark impacts.
- Writes a short CSV report to C:\Quant\analysis\trade_recon_2025-04-04.csv
"""
from pathlib import Path
import pandas as pd
import numpy as np
import sys

DATE = pd.to_datetime("2025-04-04").date()
DATA_ROOT = Path(r"C:\Quant\data")
ANALYTICS = DATA_ROOT / "analytics"
OUT = Path(r"C:\Quant\analysis")
OUT.mkdir(parents=True, exist_ok=True)

# Candidate trade file locations (script will try each)
TRADE_CANDIDATES = [
    ANALYTICS / "trades.csv",
    ANALYTICS / "executions.csv",
    DATA_ROOT / "trades.csv",
    DATA_ROOT / "analytics" / "trades.csv",
    DATA_ROOT / "backtest" / "backtest_trades.csv",
]

# Per-ticker mark impacts file produced earlier
MARK_FILE = OUT / "per_ticker_pnl_2025-04-04.csv"

def find_and_load_trades(candidates):
    for p in candidates:
        if not p.exists():
            continue
        try:
            df = pd.read_csv(p, parse_dates=True, infer_datetime_format=True)
            print("Loaded trades from", p)
            return df, p
        except Exception:
            try:
                df = pd.read_csv(p)
                print("Loaded trades (no parse) from", p)
                return df, p
            except Exception:
                continue
    return None, None

def normalize_trades(df):
    # normalize common column names
    cols = {c.lower(): c for c in df.columns}
    # unify date column
    date_col = next((cols[k] for k in cols if k in ("date","trade_date","timestamp","time")), None)
    if date_col is None:
        raise ValueError("No date/timestamp column found in trades")
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    # unify ticker
    ticker_col = next((cols[k] for k in cols if k in ("ticker","symbol","asset")), None)
    if ticker_col is None:
        raise ValueError("No ticker column found in trades")
    # unify qty and price
    qty_col = next((cols[k] for k in cols if k in ("qty","quantity","shares")), None)
    price_col = next((cols[k] for k in cols if k in ("price","px","execution_price")), None)
    side_col = next((cols[k] for k in cols if k in ("side","buy_sell")), None)
    # coerce numeric
    if qty_col:
        df[qty_col] = pd.to_numeric(df[qty_col], errors='coerce')
    if price_col:
        df[price_col] = pd.to_numeric(df[price_col], errors='coerce')
    return df, date_col, ticker_col, qty_col, price_col, side_col

def compute_realized_by_ticker(df, date_col, ticker_col, qty_col, price_col, side_col):
    # filter to date
    df_day = df[df[date_col].dt.date == DATE].copy()
    if df_day.empty:
        return pd.Series(dtype=float), df_day
    # signed qty: BUY positive, SELL negative if side exists, else use sign of qty
    if side_col and side_col in df_day.columns:
        side_map = {k:1 for k in []}  # placeholder, not used if side values vary
        # try common conventions
        df_day['signed_qty'] = df_day[qty_col].where(df_day[side_col].str.upper().isin(['BUY','B']), -df_day[qty_col])
    else:
        df_day['signed_qty'] = df_day[qty_col].fillna(0)
    # realized notional and simple realized pnl proxy: signed_qty * price
    df_day['signed_notional'] = df_day['signed_qty'] * df_day[price_col]
    # group by ticker
    realized_notional = df_day.groupby(ticker_col)['signed_notional'].sum()
    # simple realized pnl proxy: if we have a 'side' convention this is a placeholder; real PnL needs position accounting
    realized_qty = df_day.groupby(ticker_col)['signed_qty'].sum()
    return realized_notional, df_day

def load_mark_impacts(path):
    if not path.exists():
        return None
    try:
        m = pd.read_csv(path, index_col=0, squeeze=True)
        # ensure Series
        if isinstance(m, pd.DataFrame):
            m = m.iloc[:,0]
        m.index = m.index.astype(str)
        return m.astype(float)
    except Exception:
        return None

def main():
    trades, trades_path = find_and_load_trades(TRADE_CANDIDATES)
    if trades is None:
        print("No trades file found in candidates. Run this PowerShell to list files under data analytics:")
        print(r'Get-ChildItem -Path C:\Quant\data\analytics -File | Select Name,Length,LastWriteTime | Format-Table -AutoSize')
        sys.exit(1)

    try:
        trades, date_col, ticker_col, qty_col, price_col, side_col = normalize_trades(trades)
    except Exception as e:
        print("Could not normalize trades file:", e)
        sys.exit(1)

    realized_by_ticker, trades_day = compute_realized_by_ticker(trades, date_col, ticker_col, qty_col, price_col, side_col)
    if trades_day.empty:
        print(f"No trades on {DATE} found in {trades_path}")
    else:
        print(f"Found {len(trades_day)} trade rows on {DATE} in {trades_path}")

    mark = load_mark_impacts(MARK_FILE)
    if mark is None:
        print("Mark impacts file not found at", MARK_FILE)
        # still write realized_by_ticker to CSV for inspection
        realized_by_ticker.to_csv(OUT / f"realized_notional_{DATE}.csv", header=["signed_notional"])
        print("Wrote realized notional to", OUT / f"realized_notional_{DATE}.csv")
        sys.exit(0)

    # align tickers and produce reconciliation table
    tickers = sorted(set(realized_by_ticker.index.astype(str)).union(set(mark.index.astype(str))))
    recon = pd.DataFrame(index=tickers)
    recon['realized_notional'] = realized_by_ticker.reindex(tickers).fillna(0).astype(float)
    recon['mark_impact'] = mark.reindex(tickers).fillna(0).astype(float)
    # simple ratio and difference
    recon['diff'] = recon['realized_notional'] - recon['mark_impact']
    recon['abs_diff'] = recon['diff'].abs()
    recon = recon.sort_values('abs_diff', ascending=False)

    out_file = OUT / f"trade_recon_{DATE}.csv"
    recon.to_csv(out_file)
    print("Wrote reconciliation to", out_file)
    print("\nTop mismatches (realized vs mark):")
    print(recon.head(20))

if __name__ == '__main__':
    main()