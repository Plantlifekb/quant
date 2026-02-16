import os,sys,glob,pandas as pd
picks_path = r"C:\Quant\data\analytics\quant_weekly_picks_quant_v1.parquet"
prices_path = r"C:\Quant\data\canonical\prices.parquet"
out_picks = r"C:\Quant\data\analytics\quant_weekly_picks_quant_v1.price_backfilled.parquet"
backup = r"C:\Quant\data\analytics\quant_weekly_picks_quant_v1.backup.parquet"
if not os.path.exists(picks_path):
    print("ERROR: picks parquet not found:", picks_path); sys.exit(1)
if not os.path.exists(prices_path):
    print("ERROR: prices file not found:", prices_path); sys.exit(2)
df_p = pd.read_parquet(picks_path)
df_prices = pd.read_parquet(prices_path)
# normalize
df_p['as_of_date'] = pd.to_datetime(df_p.get('as_of_date', df_p.get('date'))).dt.normalize()
df_p['ticker'] = df_p['ticker'].astype(str).str.upper().str.strip()
df_prices['date'] = pd.to_datetime(df_prices['date']).dt.normalize()
df_prices['ticker'] = df_prices['ticker'].astype(str).str.upper().str.strip()
# build lookup of (ticker,date)->price
price_lookup = df_prices[['date','ticker','price']].copy()
price_lookup = price_lookup.dropna(subset=['date','ticker'])
price_lookup = price_lookup.set_index(['ticker','date'])['price']
# helper to get monday and friday for a given as_of_date
def week_mon_fri(d):
    d = pd.to_datetime(d).date()
    monday = d - pd.Timedelta(days=d.weekday())
    friday = monday + pd.Timedelta(days=4)
    return pd.to_datetime(monday), pd.to_datetime(friday)
monday_vals=[]; friday_vals=[]
for _, r in df_p.iterrows():
    mon, fri = week_mon_fri(r['as_of_date'])
    t = r['ticker']
    mo = price_lookup.get((t, mon), None) if (t,mon) in price_lookup.index else None
    fr = price_lookup.get((t, fri), None) if (t,fri) in price_lookup.index else None
    monday_vals.append(mo)
    friday_vals.append(fr)
df_p['monday_open'] = monday_vals
df_p['friday_close'] = friday_vals
# backup and write
if not os.path.exists(backup):
    os.rename(picks_path, backup)
    print("Backed up original picks to", backup)
df_p.to_parquet(out_picks, index=False)
print("Wrote backfilled picks to", out_picks)
# run validator (existing script)
validator = r"C:\Quant\scripts\validation\weekly_validation_single_script.py"
if not os.path.exists(validator):
    print("ERROR: validator script not found:", validator); sys.exit(3)
os.system(f'"{sys.executable}" "{validator}" --parquet "{out_picks}" --out_dir "C:\\Quant\\scripts\\validation" --days 60')
print("Done.")
