#!/usr/bin/env python3
"""
quant_weekly_report_last_2m_with_combined.py

Produces:
 - weekly_picks_report_last_2m.csv  (per-pick rows with monday_open, friday_close, mon2fri_ret, adj_ret)
 - weekly_picks_summary_last_2m.csv (per-week summary including combined_unweighted and combined_weighted)

Usage examples:
  python quant_weekly_report_last_2m_with_combined.py
  python quant_weekly_report_last_2m_with_combined.py --input df_perf_export.csv --days 60
  python quant_weekly_report_last_2m_with_combined.py --tickers AAPL,MSFT --use-signed-weights
"""
import os, sys, argparse
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import yfinance as yf

# --- CLI args
p = argparse.ArgumentParser(description="Quant Weekly last-2-months report with combined metrics")
p.add_argument("--input", default="df_perf_export.csv", help="Input CSV filename (default df_perf_export.csv)")
p.add_argument("--out_dir", default=".", help="Output directory (default current folder)")
p.add_argument("--days", type=int, default=60, help="Lookback days (default 60). If no picks found, script will extend to 90.")
p.add_argument("--tickers", help="Optional comma-separated list of tickers to include (e.g., AAPL,MSFT).")
p.add_argument("--use-signed-weights", action="store_true", help="Use signed weights for combined_weighted instead of absolute weights.")
args = p.parse_args()

INPUT_CSV = os.path.abspath(args.input)
OUT_PER_PICK = os.path.join(args.out_dir, "weekly_picks_report_last_2m.csv")
OUT_PER_WEEK = os.path.join(args.out_dir, "weekly_picks_summary_last_2m.csv")

if not os.path.exists(INPUT_CSV):
    print("ERROR: input CSV not found:", INPUT_CSV)
    sys.exit(1)

# --- Load picks
df = pd.read_csv(INPUT_CSV)
required = {"date", "ticker"}
if not required.issubset(set(df.columns)):
    print("ERROR: input CSV must contain columns:", required)
    sys.exit(1)

df = df.copy()
df['date'] = pd.to_datetime(df['date']).dt.normalize()
df['ticker'] = df['ticker'].astype(str).str.upper()

# optional ticker filter
if args.tickers:
    keep = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    df = df[df['ticker'].isin(keep)]
    if df.empty:
        print("No picks after applying ticker filter:", keep)
        sys.exit(0)

# lookback window
today = pd.Timestamp.now().normalize()
cutoff = today - pd.Timedelta(days=args.days)
df_recent = df[df['date'] >= cutoff]
if df_recent.empty:
    # fallback to 90 days
    cutoff = today - pd.Timedelta(days=90)
    df_recent = df[df['date'] >= cutoff]
    print("No picks in last", args.days, "days; falling back to last 90 days.")
df = df_recent.copy()
if df.empty:
    print("No picks found in the fallback window. Exiting.")
    sys.exit(0)

# helpers
def week_monday_friday(anchor_date):
    d = pd.to_datetime(anchor_date)
    monday = d - pd.Timedelta(days=d.weekday())
    friday = monday + pd.Timedelta(days=4)
    return monday.date(), friday.date()

def fetch_ohlc(ticker, start_date, end_date):
    start = pd.to_datetime(start_date).strftime("%Y-%m-%d")
    end = (pd.to_datetime(end_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        data = yf.download(ticker, start=start, end=end, progress=False, threads=True, auto_adjust=False)
        if data is None or data.empty:
            return None
        data.index = pd.to_datetime(data.index).date
        return data
    except Exception as e:
        print(f"yfinance error for {ticker} {start}->{end}: {e}")
        return None

def to_scalar(x):
    if x is None:
        return None
    if isinstance(x, pd.Series):
        if x.empty:
            return None
        x = x.iloc[0]
    try:
        return float(x)
    except Exception:
        return None

rows = []
grouped = df.groupby(['date','ticker'])
print(f"Processing {len(grouped)} unique picks...")

for (week_date, ticker), group in grouped:
    monday, friday = week_monday_friday(week_date)
    ohlc = fetch_ohlc(ticker, monday, friday)

    mon_open = None
    fri_close = None
    note = None

    if ohlc is None or ohlc.empty:
        note = "no price data for week"
    else:
        try:
            mon_open_raw = ohlc.loc[monday]['Open'] if monday in ohlc.index else ohlc.iloc[0]['Open']
        except Exception:
            mon_open_raw = None
        try:
            fri_close_raw = ohlc.loc[friday]['Close'] if friday in ohlc.index else ohlc.iloc[-1]['Close']
        except Exception:
            fri_close_raw = None
        mon_open = to_scalar(mon_open_raw)
        fri_close = to_scalar(fri_close_raw)

    if mon_open is None or fri_close is None or mon_open == 0 or pd.isna(mon_open) or pd.isna(fri_close):
        ret = np.nan
    else:
        ret = (fri_close / mon_open) - 1.0

    row = group.iloc[0]
    side = row['side'] if 'side' in row.index else None
    score = row['score'] if 'score' in row.index else None
    weight = row['weight'] if 'weight' in row.index else None

    rows.append({
        "week": week_date.strftime("%Y-%m-%d"),
        "ticker": ticker,
        "monday": str(monday),
        "friday": str(friday),
        "monday_open": mon_open,
        "friday_close": fri_close,
        "mon2fri_ret": ret,
        "mon2fri_ret_pct": (ret * 100) if pd.notna(ret) else np.nan,
        "side": side,
        "score": score,
        "weight": weight,
        "note": note
    })

out = pd.DataFrame(rows)
# compute adj_ret: long keeps sign, short flips sign; if side missing but score present, infer side from score sign
def compute_adj_ret(r):
    ret = r['mon2fri_ret']
    s = str(r.get('side')).lower() if pd.notna(r.get('side')) else None
    if pd.isna(ret):
        return np.nan
    if s in ('long','short'):
        return ret if s == 'long' else -ret
    # fallback to score sign
    sc = r.get('score')
    try:
        scf = float(sc)
        if scf > 0:
            return ret
        elif scf < 0:
            return -ret
    except Exception:
        pass
    return np.nan

out['adj_ret'] = out.apply(compute_adj_ret, axis=1)
out['adj_ret_pct'] = out['adj_ret'] * 100

# per-week summary
per_week = out.groupby('week').agg(
    picks=('ticker','count'),
    valid_picks=('mon2fri_ret', lambda s: int(s.notna().sum()))
).reset_index()

# unweighted mean of adj_ret
per_week['combined_unweighted'] = per_week['week'].apply(lambda w: out[out['week']==w]['adj_ret'].dropna().mean())
# weighted mean using weights
if 'weight' in out.columns:
    out['weight_num'] = pd.to_numeric(out['weight'], errors='coerce')
    if args.use_signed_weights:
        out['w_for_weighted'] = out['weight_num']
    else:
        out['w_for_weighted'] = out['weight_num'].abs()
    def weighted_mean(sub):
        sub = sub.dropna(subset=['adj_ret','w_for_weighted'])
        if sub.empty:
            return np.nan
        num = (sub['adj_ret'] * sub['w_for_weighted']).sum()
        den = sub['w_for_weighted'].sum()
        return num/den if den != 0 else np.nan
    per_week['combined_weighted'] = per_week['week'].apply(lambda w: weighted_mean(out[out['week']==w]))
else:
    per_week['combined_weighted'] = np.nan

# drawdown: worst single pick adj_ret or mon2fri_ret? Use worst single pick price return (mon2fri_ret) as downside
per_week['drawdown'] = per_week['week'].apply(lambda w: out[out['week']==w]['mon2fri_ret'].dropna().min() if not out[out['week']==w]['mon2fri_ret'].dropna().empty else np.nan)

# percent columns
per_week['combined_unweighted_pct'] = per_week['combined_unweighted'] * 100
per_week['combined_weighted_pct'] = per_week['combined_weighted'] * 100
per_week['drawdown_pct'] = per_week['drawdown'] * 100

# write outputs
out.to_csv(OUT_PER_PICK, index=False, float_format="%.8f")
per_week.to_csv(OUT_PER_WEEK, index=False, float_format="%.8f")

print("Wrote per-pick report:", OUT_PER_PICK)
print("Wrote per-week summary:", OUT_PER_WEEK)

# human readable summary
pd.set_option('display.width', 200)
print("\nPer-week summary:")
print(per_week[['week','picks','valid_picks','combined_unweighted_pct','combined_weighted_pct','drawdown_pct']].to_string(index=False))

print("\nSample per-pick rows (last 20):")
print(out.tail(20).to_string(index=False))