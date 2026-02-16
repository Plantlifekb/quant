# validate_weekly_picks.py
import sys, os, math
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import pytz

# adjust path to where your dashboard_app.py lives
sys.path.insert(0, r"C:\Quant\scripts\dashboard")
try:
    import dashboard_app as app
except Exception as e:
    print("ERROR: failed to import dashboard_app:", e)
    raise SystemExit(1)

df = getattr(app, "df_perf", None)
if df is None or len(df) == 0:
    print("ERROR: df_perf not found or empty in dashboard_app")
    raise SystemExit(1)

# Ensure date columns
if "date" not in df.columns:
    print("ERROR: df_perf missing 'date' column. Columns:", list(df.columns))
    raise SystemExit(1)

# Normalize date and week fields
df = df.copy()
df['date'] = pd.to_datetime(df['date']).dt.normalize()  # week anchor date
# If you have a separate 'week' column with range strings, prefer 'date' as anchor

# Helper to get Monday and Friday for a given anchor date
def week_monday_friday(anchor_date):
    # anchor_date is a Timestamp representing the week (e.g., 2026-01-20)
    # find Monday of that ISO week
    d = pd.to_datetime(anchor_date)
    # If anchor is already Monday, use it; otherwise compute Monday of that week
    monday = d - pd.Timedelta(days=(d.weekday()))  # weekday: Monday=0
    friday = monday + pd.Timedelta(days=4)
    return monday.date(), friday.date()

# Function to fetch OHLC for ticker between dates (inclusive)
def fetch_ohlc(ticker, start_date, end_date):
    # yfinance expects strings
    start = pd.to_datetime(start_date).strftime("%Y-%m-%d")
    end = (pd.to_datetime(end_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")  # inclusive
    try:
        data = yf.download(ticker, start=start, end=end, progress=False, threads=True)
        if data is None or data.empty:
            return None
        # ensure timezone naive dates
        data.index = pd.to_datetime(data.index).date
        return data
    except Exception as e:
        print(f"Warning: failed to download {ticker} {start}->{end}: {e}")
        return None

rows = []
# iterate unique (date, ticker) picks
group_cols = ['date', 'ticker']
if 'ticker' not in df.columns:
    print("ERROR: df_perf missing 'ticker' column")
    raise SystemExit(1)

for (week_date, ticker), group in df.groupby(group_cols):
    monday, friday = week_monday_friday(week_date)
    ohlc = fetch_ohlc(ticker, monday, friday)
    if ohlc is None or monday not in ohlc.index or friday not in ohlc.index:
        # try tolerant fallback: use first available day in week for open and last available for close
        if ohlc is None or ohlc.empty:
            rows.append({
                "week": week_date.strftime("%Y-%m-%d"),
                "ticker": ticker,
                "monday": str(monday),
                "friday": str(friday),
                "mon_open": None,
                "fri_close": None,
                "mon2fri_ret": None,
                "note": "no price data"
            })
            continue
        mon_open = ohlc.iloc[0]['Open']
        fri_close = ohlc.iloc[-1]['Close']
    else:
        mon_open = float(ohlc.loc[monday]['Open'])
        fri_close = float(ohlc.loc[friday]['Close'])

    if mon_open is None or fri_close is None or mon_open == 0 or math.isnan(mon_open) or math.isnan(fri_close):
        ret = None
    else:
        ret = (fri_close / mon_open) - 1.0

    # pick info: prefer 'side' then 'score'
    side = group['side'].iloc[0] if 'side' in group.columns else None
    score = group['score'].iloc[0] if 'score' in group.columns else None
    weight = group['weight'].iloc[0] if 'weight' in group.columns else None

    # determine correctness
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
        # sign match
        try:
            correct = np.sign(float(score)) == np.sign(ret)
            # ignore zero score or zero ret as non-contributory
            if float(score) == 0 or ret == 0:
                correct = None
            else:
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
        "note": None
    })

out = pd.DataFrame(rows)

# Per-week summaries
per_week = out.groupby('week').agg(
    picks=('ticker', 'count'),
    correct_count=('correct', lambda s: int(s.dropna().sum())),
    valid_count=('correct', lambda s: int(s.notna().sum()))
).reset_index()
per_week['accuracy_unweighted'] = per_week.apply(lambda r: (r['correct_count'] / r['valid_count']) if r['valid_count']>0 else None, axis=1)

# Weighted accuracy per week
def weighted_acc_for_week(w):
    sub = out[out['week']==w]
    if sub.empty or 'weight' not in sub.columns:
        return None
    # only consider rows with non-null correct and numeric weight
    sub2 = sub[sub['correct'].notna() & sub['weight'].notna()]
    if sub2.empty:
        return None
    num = (sub2['correct'].astype(float) * sub2['weight'].abs()).sum()
    den = sub2['weight'].abs().sum()
    return (num/den) if den>0 else None

per_week['accuracy_weighted'] = per_week['week'].apply(weighted_acc_for_week)

# Overall 4-week averages (last 4 weeks present)
last_weeks = sorted(per_week['week'].dropna().unique())[-4:]
last_df = per_week[per_week['week'].isin(last_weeks)]
overall_unweighted_4w = last_df['accuracy_unweighted'].dropna().mean() if not last_df['accuracy_unweighted'].dropna().empty else None
overall_weighted_4w = last_df['accuracy_weighted'].dropna().mean() if not last_df['accuracy_weighted'].dropna().empty else None

# Print results
pd.set_option('display.width', 200)
print("\nPer-ticker results (sample):")
print(out.to_string(index=False))

print("\nPer-week summary:")
print(per_week.to_string(index=False))

print("\nLast 4 weeks used:", last_weeks)
print("Computed % Accuracy (4w avg) unweighted:", "{:.2%}".format(overall_unweighted_4w) if overall_unweighted_4w is not None else "n/a")
print("Computed % Accuracy (4w avg) weighted:", "{:.2%}".format(overall_weighted_4w) if overall_weighted_4w is not None else "n/a")

# Save CSV for review
out_path = os.path.join(r"C:\Quant\scripts\dashboard", "weekly_picks_validation.csv")
out.to_csv(out_path, index=False)
print("\nSaved detailed per-ticker validation to", out_path)