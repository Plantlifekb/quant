#!/usr/bin/env python3
"""
generate_verification_report.py
- Computes Bias, RMSE, Hit rate, Correlation, Coverage for longonly and longshort
- Lists weeks where predicted != realized and offending tickers
- Writes verification_report.md to outputs/verification/
- Prints a concise summary to stdout
Run: python .\scripts\analytics\generate_verification_report.py
"""
import pandas as pd
from pathlib import Path
import numpy as np

ROOT = Path.cwd()
outdir = ROOT / "outputs" / "verification"
outdir.mkdir(parents=True, exist_ok=True)

# input files
weekly_file = outdir / "weekly_portfolio_predicted_vs_realized.csv"
pred_lo = outdir / "predicted_vs_picks_weekly_longonly.csv"
real_lo = outdir / "realized_vs_picks_weekly_longonly.csv"
pred_ls = outdir / "predicted_vs_picks_weekly_longshort.csv"
real_ls = outdir / "realized_vs_picks_weekly_longshort.csv"

def safe_read(p):
    if not p.exists():
        raise FileNotFoundError(f"Missing file: {p}")
    return pd.read_csv(p)

# metrics helpers
def bias_rmse(pred, real):
    diff = pred - real
    bias = diff.mean()
    rmse = np.sqrt((diff**2).mean())
    return float(bias), float(rmse)

def hit_rate(pred, real):
    # hit if sign(pred) == sign(real); treat zeros as misses
    s_pred = np.sign(pred)
    s_real = np.sign(real)
    hits = (s_pred == s_real) & (s_pred != 0)
    return float(hits.sum() / len(pred))

def coverage(pred, tradeable_flag_col=None):
    # coverage = fraction of picks with non-null realized value (or tradeable_flag==True)
    if tradeable_flag_col is not None:
        return float((tradeable_flag_col.astype(bool).sum()) / len(tradeable_flag_col))
    return None

def corr(pred, real):
    if len(pred) < 2:
        return float('nan')
    return float(np.corrcoef(pred, real)[0,1])

# load weekly aggregate
weekly = safe_read(weekly_file)
# ensure numeric
weekly['predicted_portfolio_gain'] = pd.to_numeric(weekly['predicted_portfolio_gain'], errors='coerce')
weekly['realized_portfolio_gain'] = pd.to_numeric(weekly['realized_portfolio_gain'], errors='coerce')

# identify mismatched weeks
mismatch = weekly[weekly['predicted_portfolio_gain'].fillna(0) != weekly['realized_portfolio_gain'].fillna(0)]
mismatch_summary = mismatch[['week_start','strategy','predicted_portfolio_gain','realized_portfolio_gain','n_untradeable']]

# helper to compute pick-level metrics
def compute_pick_metrics(pred_file, real_file, strategy_name):
    p = safe_read(pred_file)
    r = safe_read(real_file)
    # merge on week_start,ticker if not already aligned
    if set(['week_start','ticker']).issubset(p.columns) and set(['week_start','ticker']).issubset(r.columns):
        merged = p.merge(r, on=['week_start','ticker'], how='outer', suffixes=('_pred','_real'))
    else:
        # fallback: align by index
        merged = pd.concat([p, r], axis=1)
    # coerce numeric gains
    for col in ['predicted_week_gain','realized_week_gain','target_weight']:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors='coerce').fillna(0.0)
    # compute metrics per week and overall
    results = {}
    # overall metrics
    pred = merged.get('predicted_week_gain', pd.Series(dtype=float)).fillna(0.0)
    real = merged.get('realized_week_gain', pd.Series(dtype=float)).fillna(0.0)
    bias, rmse = bias_rmse(pred, real)
    hr = hit_rate(pred, real)
    c = corr(pred, real)
    # coverage: fraction of picks with realized non-null (or tradeable_flag true)
    coverage_val = None
    if 'tradeable_flag' in merged.columns:
        coverage_val = float(merged['tradeable_flag'].astype(bool).sum() / len(merged))
    else:
        coverage_val = float((~merged['realized_week_gain'].isna()).sum() / len(merged))
    results['strategy'] = strategy_name
    results['n_picks'] = len(merged)
    results['bias'] = bias
    results['rmse'] = rmse
    results['hit_rate'] = hr
    results['correlation'] = c
    results['coverage'] = coverage_val
    # top mismatches by absolute difference
    merged['abs_diff'] = (merged.get('predicted_week_gain',0) - merged.get('realized_week_gain',0)).abs()
    top_mismatch = merged.sort_values('abs_diff', ascending=False).head(20)
    results['top_mismatch'] = top_mismatch[['week_start','ticker','target_weight','predicted_week_gain','realized_week_gain','abs_diff']].fillna('')
    return results

# compute for longonly
res_lo = compute_pick_metrics(pred_lo, real_lo, 'longonly')
# compute for longshort
res_ls = compute_pick_metrics(pred_ls, real_ls, 'longshort')

# write verification_report.md
report = []
report.append("# Verification Report\n")
report.append("## Summary\n")
report.append(f"- **Total weeks processed**: {len(weekly)}\n")
report.append(f"- **Weeks with predicted != realized**: {len(mismatch)}\n")
report.append("\n## Weekly mismatches\n")
if len(mismatch) == 0:
    report.append("No mismatches found between predicted and realized portfolio gains.\n")
else:
    report.append("| week_start | strategy | predicted_portfolio_gain | realized_portfolio_gain | n_untradeable |\n")
    report.append("|---|---:|---:|---:|---:|\n")
    for _, row in mismatch_summary.iterrows():
        report.append(f"| {row.week_start} | {row.strategy} | {row.predicted_portfolio_gain} | {row.realized_portfolio_gain} | {row.n_untradeable} |\n")

def add_strategy_section(res):
    report.append(f"\n## Strategy {res['strategy']}\n")
    report.append(f"- **Number of picks**: {res['n_picks']}\n")
    report.append(f"- **Bias (mean predicted - realized)**: {res['bias']:.6f}\n")
    report.append(f"- **RMSE**: {res['rmse']:.6f}\n")
    report.append(f"- **Hit rate**: {res['hit_rate']:.3f}\n")
    report.append(f"- **Correlation**: {res['correlation']:.3f}\n")
    report.append(f"- **Coverage**: {res['coverage']:.3f}\n")
    report.append("\n### Top pick mismatches\n")
    report.append("| week_start | ticker | target_weight | predicted_week_gain | realized_week_gain | abs_diff |\n")
    report.append("|---|---|---:|---:|---:|---:|\n")
    for _, r in res['top_mismatch'].iterrows():
        report.append(f"| {r.week_start} | {r.ticker} | {r.target_weight} | {r.predicted_week_gain} | {r.realized_week_gain} | {r.abs_diff} |\n")

add_strategy_section(res_lo)
add_strategy_section(res_ls)

report_text = "\n".join(report)
(report_path := outdir / "verification_report.md").write_text(report_text, encoding="utf8")
print("Wrote verification_report.md to", report_path)

# print concise console summary
print("\n=== Console summary ===")
print(f"Total weeks: {len(weekly)}; mismatched weeks: {len(mismatch)}")
print("\nLongonly metrics:")
print(f"  Bias: {res_lo['bias']:.6f}, RMSE: {res_lo['rmse']:.6f}, Hit rate: {res_lo['hit_rate']:.3f}, Corr: {res_lo['correlation']:.3f}, Coverage: {res_lo['coverage']:.3f}")
print("\nLongshort metrics:")
print(f"  Bias: {res_ls['bias']:.6f}, RMSE: {res_ls['rmse']:.6f}, Hit rate: {res_ls['hit_rate']:.3f}, Corr: {res_ls['correlation']:.3f}, Coverage: {res_ls['coverage']:.3f}")

# print top 10 mismatches overall
print("\nTop 10 pick mismatches (longshort):")
print(res_ls['top_mismatch'].head(10).to_string(index=False))