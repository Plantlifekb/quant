#!/usr/bin/env python3
# validate_all_weekly_picks.py
import sys, os, math
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import importlib

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FALLBACK = os.path.join(SCRIPT_DIR, "df_perf_export.csv")
OUT_PER_PICK = os.path.join(SCRIPT_DIR, "weekly_picks_validation_all.csv")
OUT_PER_WEEK = os.path.join(SCRIPT_DIR, "weekly_picks_summary.csv")

# Try to import dashboard_app and get df_perf, otherwise load CSV fallback
df = None
sys.path.insert(0, r"C:\Quant\scripts\dashboard")
try:
    app = importlib.import_module("dashboard_app")
    df = getattr(app, "df_perf", None)
    if df is None and hasattr(app, "get_df_perf"):
        try:
            df = app.get_df_perf()
        except Exception:
            df = None
except Exception:
    df = None

if df is None:
    if os.path.exists(CSV_FALLBACK):
        df = pd.read_csv(CSV_FALLBACK)
        print("Loaded df_perf from CSV fallback:", CSV_FALLBACK)
    else:
        print("ERROR: df_perf not found in dashboard_app and CSV fallback missing:", CSV_FALLBACK)
        sys.exit(1)

# Normalize and basic checks
if "date" not in df.columns or "ticker" not in df.columns:
    print("ERROR: df_perf must contain 'date' and 'ticker' columns. Columns:", list(df.columns))
    sys.exit(1)

df = df.copy()
df['date'] = pd.to_datetime(df['date']).dt.normalize()
df['ticker'] = df['ticker'].astype(str).str.upper()

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
        print("yfinance download error for", ticker, start, end, e)
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
print("Processing", len(grouped), "unique picks...")

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
        ret = None
    else:
        ret = (fri_close / mon_open) - 1.0

    row = group.iloc[0]
    side = row['side'] if 'side' in row.index else None
    score = row['score'] if 'score' in row.index else None
    weight = row['weight'] if 'weight' in row.index else None

    correct = None
    method = None
    if side is not None and pd.notna(ret):
        s = str(side).strip().lower()
        if s == "long":
            correct = ret > 0
            method = "side_vs_ret"
        elif s == "short":
            correct = ret < 0
            method = "side_vs_ret"
    if correct is None and score is not None and pd.notna(ret):
        try:
            sc = float(score)
            if sc == 0 or ret == 0:
                correct = None
            else:
                correct = np.sign(sc) == np.sign(ret)
                method = "score_sign_vs_ret"
        except Exception:
            correct = None

    rows.append({
        "week": week_date.strftime("%Y-%m-%d"),
        "ticker": ticker,
        "monday": str(monday),
        "friday": str(friday),
        "mon_open": mon_open,
        "fri_close": fri_close,
        "mon2fri_ret": ret,
        "side": side,
        "score": score,
        "weight": weight,
        "correct": bool(correct) if correct is not None else None,
        "method": method,
        "note": note
    })

out = pd.DataFrame(rows)
out.to_csv(OUT_PER_PICK, index=False)
print("Wrote per-pick CSV:", OUT_PER_PICK)

# Per-week summary
per_week = out.groupby('week').agg(
    picks=('ticker','count'),
    correct_count=('correct', lambda s: int(s.dropna().sum())),
    valid_count=('correct', lambda s: int(s.notna().sum()))
).reset_index()
per_week['accuracy_unweighted'] = per_week.apply(lambda r: (r['correct_count']/r['valid_count']) if r['valid_count']>0 else None, axis=1)

def weighted_acc_for_week(w):
    sub = out[out['week']==w]
    if sub.empty or 'weight' not in sub.columns:
        return None
    sub2 = sub[sub['correct'].notna() & sub['weight'].notna()]
    if sub2.empty:
        return None
    num = (sub2['correct'].astype(float) * sub2['weight'].abs()).sum()
    den = sub2['weight'].abs().sum()
    return (num/den) if den>0 else None

per_week['accuracy_weighted'] = per_week['week'].apply(weighted_acc_for_week)
per_week.to_csv(OUT_PER_WEEK, index=False)
print("Wrote per-week summary CSV:", OUT_PER_WEEK)

# Print short summary
last_weeks = sorted(per_week['week'].dropna().unique())[-4:]
print("Last 4 weeks used:", last_weeks)
print("Sample per-pick rows (last 10):")
print(out.tail(10).to_string(index=False))