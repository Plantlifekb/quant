# auto_backfill_and_validate.py
# Finds a daily prices file under C:\Quant\data, backfills monday_open/friday_close into picks parquet,
# runs the validation script, and prints the summary.
import os, sys, glob, pandas as pd
picks_path = r"C:\Quant\data\analytics\quant_weekly_picks_quant_v1.parquet"
out_picks_path = r"C:\Quant\data\analytics\quant_weekly_picks_quant_v1.backfilled.parquet"
backup_path = r"C:\Quant\data\analytics\quant_weekly_picks_quant_v1.backup.parquet"
# search for candidate price files
search_root = r"C:\Quant\data"
candidates = []
for pat in ("**/daily_prices*.parquet","**/prices*.parquet","**/*daily*.parquet","**/daily_prices*.csv","**/prices*.csv"):
    candidates += glob.glob(os.path.join(search_root,pat), recursive=True)
candidates = sorted(set(candidates), key=os.path.getmtime, reverse=True)
if not os.path.exists(picks_path):
    print("ERROR: picks parquet not found:", picks_path); sys.exit(1)
if not candidates:
    print("ERROR: no candidate daily prices files found under", search_root)
    print("Searched patterns: daily_prices*.parquet, prices*.parquet, *daily*.parquet, daily_prices*.csv, prices*.csv")
    sys.exit(2)
prices_path = candidates[0]
print("Using prices file:", prices_path)
# load picks
df_p = pd.read_parquet(picks_path)
if 'as_of_date' not in df_p.columns:
    if 'date' in df_p.columns:
        df_p['as_of_date'] = pd.to_datetime(df_p['date']).dt.normalize()
    else:
        print("ERROR: picks parquet missing as_of_date"); sys.exit(3)
df_p['as_of_date'] = pd.to_datetime(df_p['as_of_date']).dt.normalize()
# load prices (parquet or csv)
if prices_path.lower().endswith('.parquet'):
    df_prices = pd.read_parquet(prices_path)
else:
    df_prices = pd.read_csv(prices_path)
# normalize price columns
cols = [c.lower() for c in df_prices.columns]
# try to find date, ticker, open, close columns
date_col = next((c for c in df_prices.columns if c.lower() in ('date','trade_date','dt')), None)
ticker_col = next((c for c in df_prices.columns if c.lower() in ('ticker','symbol')), None)
open_col = next((c for c in df_prices.columns if c.lower() in ('open','o','open_price','open_px')), None)
close_col = next((c for c in df_prices.columns if c.lower() in ('close','c','close_price','close_px')), None)
if not date_col or not ticker_col or not open_col or not close_col:
    print("ERROR: prices file missing required columns. Found columns:", list(df_prices.columns))
    sys.exit(4)
df_prices[date_col] = pd.to_datetime(df_prices[date_col]).dt.normalize()
# helper to get monday and friday dates for a given as_of_date (week)
def week_monday_friday(as_of):
    # as_of is normalized date (likely a Friday). compute that week's Monday and Friday
    d = pd.to_datetime(as_of).date()
    # find Monday of that ISO week
    monday = d - pd.Timedelta(days=d.weekday())
    friday = monday + pd.Timedelta(days=4)
    return pd.to_datetime(monday), pd.to_datetime(friday)
# build a lookup of (ticker, date) -> open/close
df_prices_lookup = df_prices[[date_col, ticker_col, open_col, close_col]].copy()
df_prices_lookup.columns = ['date','ticker','open','close']
df_prices_lookup['date'] = pd.to_datetime(df_prices_lookup['date']).dt.normalize()
df_prices_lookup['ticker'] = df_prices_lookup['ticker'].astype(str).str.upper().str.strip()
# prepare picks rows
df_p['ticker'] = df_p['ticker'].astype(str).str.upper().str.strip()
monday_vals = []
friday_vals = []
for idx, row in df_p.iterrows():
    mon, fri = week_monday_friday(row['as_of_date'])
    t = row['ticker']
    mo_row = df_prices_lookup[(df_prices_lookup['ticker']==t) & (df_prices_lookup['date']==mon)]
    fr_row = df_prices_lookup[(df_prices_lookup['ticker']==t) & (df_prices_lookup['date']==fri)]
    mo = float(mo_row['open'].iloc[0]) if not mo_row.empty else None
    fr = float(fr_row['close'].iloc[0]) if not fr_row.empty else None
    monday_vals.append(mo)
    friday_vals.append(fr)
df_p['monday_open'] = monday_vals
df_p['friday_close'] = friday_vals
# backup original picks parquet
if not os.path.exists(backup_path):
    os.rename(picks_path, backup_path)
    print("Backed up original picks to", backup_path)
# write backfilled parquet
df_p.to_parquet(out_picks_path, index=False)
print("Wrote backfilled picks to", out_picks_path)
# run validator script (assumes weekly_validation_single_script.py exists)
validator = r"C:\Quant\scripts\validation\weekly_validation_single_script.py"
if not os.path.exists(validator):
    print("ERROR: validator script not found:", validator); sys.exit(5)
# call validator
print("Running validator...")
os.system(f'"{sys.executable}" "{validator}" --parquet "{out_picks_path}" --out_dir "C:\\Quant\\scripts\\validation" --days 60')
print("Done.")
