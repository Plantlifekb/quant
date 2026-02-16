#!/usr/bin/env python3
"""
validate_single_pick_cli.py
Validate one weekly pick using Monday open -> Friday close.
Usage:
  python validate_single_pick_cli.py 2026-01-20 AAPL --side long
  python validate_single_pick_cli.py 2026-01-20 AAPL --score 0.45
"""
import sys, argparse, math, json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import yfinance as yf

def week_monday_friday(anchor_date):
    d = pd.to_datetime(anchor_date)
    monday = d - pd.Timedelta(days=d.weekday())  # Monday=0
    friday = monday + pd.Timedelta(days=4)
    return monday.date(), friday.date()

def fetch_ohlc(ticker, start_date, end_date):
    start = pd.to_datetime(start_date).strftime("%Y-%m-%d")
    end = (pd.to_datetime(end_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    data = yf.download(ticker, start=start, end=end, progress=False, threads=True, auto_adjust=False)
    if data is None or data.empty:
        return None
    data.index = pd.to_datetime(data.index).date
    return data

def to_scalar(x):
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None

def validate_pick_cli(week_anchor, ticker, side=None, score=None, weight=None):
    monday, friday = week_monday_friday(week_anchor)
    ohlc = fetch_ohlc(ticker, monday, friday)

    mon_open = None
    fri_close = None
    note = None

    if ohlc is None or ohlc.empty:
        note = "no price data for week"
    else:
        try:
            mon_open_raw = ohlc.loc[monday]["Open"] if monday in ohlc.index else ohlc.iloc[0]["Open"]
        except Exception:
            mon_open_raw = None
        try:
            fri_close_raw = ohlc.loc[friday]["Close"] if friday in ohlc.index else ohlc.iloc[-1]["Close"]
        except Exception:
            fri_close_raw = None
        mon_open = to_scalar(mon_open_raw)
        fri_close = to_scalar(fri_close_raw)

    if mon_open is None or fri_close is None or mon_open == 0 or pd.isna(mon_open) or pd.isna(fri_close):
        ret = None
    else:
        ret = (fri_close / mon_open) - 1.0

    correct = None
    method = None
    if side is not None and ret is not None:
        s = str(side).strip().lower()
        if s == "long":
            correct = ret > 0
            method = "side_vs_ret"
        elif s == "short":
            correct = ret < 0
            method = "side_vs_ret"
    if correct is None and score is not None and ret is not None:
        try:
            sc = float(score)
            if sc == 0 or ret == 0:
                correct = None
            else:
                correct = np.sign(sc) == np.sign(ret)
                method = "score_sign_vs_ret"
        except Exception:
            correct = None

    return {
        "week": pd.to_datetime(week_anchor).strftime("%Y-%m-%d"),
        "ticker": str(ticker).upper(),
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
    }

def main():
    p = argparse.ArgumentParser(description="Validate one weekly pick (Mon open -> Fri close).")
    p.add_argument("week", help="Week anchor date (e.g., 2026-01-20)")
    p.add_argument("ticker", help="Ticker symbol (e.g., AAPL)")
    p.add_argument("--side", choices=["long","short"], help="Optional side")
    p.add_argument("--score", type=float, help="Optional score (float)")
    p.add_argument("--weight", type=float, help="Optional weight (float)")
    args = p.parse_args()

    result = validate_pick_cli(args.week, args.ticker, side=args.side, score=args.score, weight=args.weight)
    print(json.dumps(result, indent=2, default=str))

if __name__ == "__main__":
    main()