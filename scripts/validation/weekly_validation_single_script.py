# weekly_validation_single_script.py
# Usage:
#   python weekly_validation_single_script.py --parquet "C:\Quant\data\analytics\quant_weekly_picks_quant_v1.parquet" --out_dir "C:\Quant\scripts\validation" --days 60

import argparse, os, sys
import pandas as pd

def find_price_pair(df):
    # list of candidate (monday_open, friday_close) column name pairs in order of preference
    candidates = [
        ('monday_open','friday_close'),
        ('monday_price','friday_price'),
        ('monday','friday'),
        ('open','close'),
        ('m_open','f_close'),
    ]
    for a,b in candidates:
        if a in df.columns and b in df.columns:
            return a,b
    return None, None

def safe_float(x):
    try:
        return float(x)
    except:
        return None

def main():
    p = argparse.ArgumentParser(description="Produce weekly validation CSVs from parquet")
    p.add_argument("--parquet", required=True, help="Path to quant_weekly_picks parquet")
    p.add_argument("--out_dir", required=True, help="Directory to write CSV outputs")
    p.add_argument("--days", type=int, default=60, help="Lookback window in calendar days")
    p.add_argument("--use_signed_weights", action="store_true", help="Use signed weights if weight column contains sign")
    args = p.parse_args()

    if not os.path.exists(args.parquet):
        print("ERROR: parquet not found:", args.parquet); sys.exit(1)
    os.makedirs(args.out_dir, exist_ok=True)

    df = pd.read_parquet(args.parquet)

    # normalize date column
    if 'as_of_date' in df.columns:
        df['as_of_date'] = pd.to_datetime(df['as_of_date'], errors='coerce').dt.normalize()
    elif 'date' in df.columns:
        df['as_of_date'] = pd.to_datetime(df['date'], errors='coerce').dt.normalize()
    else:
        print("ERROR: no as_of_date or date column found"); sys.exit(2)

    if 'ticker' in df.columns:
        df['ticker'] = df['ticker'].astype(str).str.upper().str.strip()
    else:
        print("ERROR: ticker column missing"); sys.exit(3)

    # default side
    if 'side' not in df.columns:
        df['side'] = 'long'
    else:
        df['side'] = df['side'].astype(str).str.lower().fillna('long')

    # ensure numeric weight
    if 'weight' in df.columns:
        df['weight'] = pd.to_numeric(df['weight'], errors='coerce').fillna(0.0)
    else:
        df['weight'] = 0.0

    # find price columns
    mo_col, fc_col = find_price_pair(df)
    if mo_col is None:
        print("WARNING: no recognized monday/friday price columns found. mon2fri_ret and adj_ret will be empty.")
    else:
        print(f"Using price columns: monday='{mo_col}' friday='{fc_col}'")

    def compute_ret(row):
        if mo_col is None:
            return None
        mo = row.get(mo_col, None)
        fc = row.get(fc_col, None)
        mo_f = safe_float(mo); fc_f = safe_float(fc)
        if mo_f is None or fc_f is None or mo_f == 0.0:
            return None
        return fc_f / mo_f - 1.0

    df['mon2fri_ret'] = df.apply(compute_ret, axis=1)

    def compute_adj(row):
        r = row['mon2fri_ret']
        if r is None:
            return None
        side = str(row.get('side','long')).lower()
        if side in ('short','s','-1'):
            return -r
        return r

    df['adj_ret'] = df.apply(compute_adj, axis=1)

    latest = df['as_of_date'].max()
    if pd.isna(latest):
        print("ERROR: no valid as_of_date values"); sys.exit(4)
    cutoff = latest - pd.Timedelta(days=args.days)
    df_window = df[df['as_of_date'] >= cutoff].copy()

    df_window['week'] = df_window['as_of_date'].dt.strftime('%Y-%m-%d')

    per_pick_cols = ['week','ticker']
    # include price columns if present
    if mo_col: per_pick_cols.append(mo_col)
    if fc_col: per_pick_cols.append(fc_col)
    per_pick_cols += ['mon2fri_ret','adj_ret','side','weight']
    per_pick = df_window[[c for c in per_pick_cols if c in df_window.columns]].copy()

    summaries = []
    for week, g in df_window.groupby('week'):
        picks = len(g)
        valid_picks = g['adj_ret'].notna().sum()
        combined_unweighted = g.loc[g['adj_ret'].notna(),'adj_ret'].mean() if valid_picks>0 else None
        weights = g['weight'].astype(float).fillna(0.0)
        adj = g['adj_ret'].astype(float)
        w = weights.abs() if not args.use_signed_weights else weights
        denom = w.sum()
        combined_weighted = (adj * w).sum() / denom if denom != 0 else None
        drawdown = g['mon2fri_ret'].dropna().min() if g['mon2fri_ret'].dropna().size>0 else None
        summaries.append({
            'week': week,
            'picks': picks,
            'valid_picks': int(valid_picks),
            'combined_unweighted': combined_unweighted,
            'combined_weighted': combined_weighted,
            'drawdown': drawdown,
            'combined_unweighted_pct': (combined_unweighted * 100) if combined_unweighted is not None else None,
            'combined_weighted_pct': (combined_weighted * 100) if combined_weighted is not None else None,
            'drawdown_pct': (drawdown * 100) if drawdown is not None else None
        })

    summary_df = pd.DataFrame(summaries).sort_values('week')

    per_pick_csv = os.path.join(args.out_dir, "weekly_picks_report_last_2m.csv")
    summary_csv = os.path.join(args.out_dir, "weekly_picks_summary_last_2m.csv")
    per_pick.to_csv(per_pick_csv, index=False)
    summary_df.to_csv(summary_csv, index=False)

    if summary_df.empty:
        print("No weeks found in the last", args.days, "days. Latest as_of_date:", latest.date())
    else:
        print("\nPer-week summary (last {} days)".format(args.days))
        print(summary_df.to_string(index=False, float_format='{:0.8f}'.format))
        print("\nSample per-pick rows (first 20):")
        print(per_pick.head(20).to_string(index=False))

if __name__ == "__main__":
    main()