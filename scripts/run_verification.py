#!/usr/bin/env python3
"""
run_verification.py

Reproducible verification routine for weekly selection outputs.
Saves artifacts to: C:/Quant/analysis
Usage:
  python C:/Quant/scripts/run_verification.py
"""

import os
import sys
import glob
import json
from datetime import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import spearmanr

np.random.seed(42)

ROOT = r"C:\Quant"
ANALYSIS_DIR = os.path.join(ROOT, "analysis")
os.makedirs(ANALYSIS_DIR, exist_ok=True)

EXPECTED = {
    "longonly": {
        "pred": os.path.join(ROOT, "outputs", "verification", "predicted_vs_picks_weekly_longonly.csv"),
        "real": os.path.join(ROOT, "outputs", "verification", "realized_vs_picks_weekly_longonly.csv"),
    },
    "longshort": {
        "pred": os.path.join(ROOT, "outputs", "verification", "predicted_vs_picks_weekly_longshort.csv"),
        "real": os.path.join(ROOT, "outputs", "verification", "realized_vs_picks_weekly_longshort.csv"),
    }
}

FALLBACK_DIRS = [
    os.path.join(ROOT, "outputs", "verification"),
    os.path.join(ROOT, "data", "analytics"),
    os.path.join(ROOT, "data"),
]


def find_fallback(patterns):
    for d in FALLBACK_DIRS:
        if not os.path.isdir(d):
            continue
        for pat in patterns:
            matches = glob.glob(os.path.join(d, pat))
            if matches:
                return matches[0]
    return None


def load_csv(path):
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        try:
            return pd.read_csv(path, encoding="latin1")
        except Exception as e:
            raise RuntimeError(f"Failed to read CSV {path}: {e}")


def standardize_columns(df):
    if df is None or df.empty:
        return df
    df = df.rename(columns={c: c.strip().lower() for c in df.columns})
    return df


def detect_and_normalize_columns(pred_df, real_df):
    """
    Normalize common alternative column names to canonical names:
      - week_start, week_begin, date -> week
      - score, predicted_rank -> predicted_score (in predicted files)
      - realized_week_gain, return -> realized_return (in realized files)
      - symbol -> ticker
    Coerce week to datetime and ticker to str.
    """
    def map_cols(df, is_pred=False, is_real=False):
        if df is None or df.empty:
            return df
        cols = set(df.columns)
        rename = {}

        # ticker
        if "symbol" in cols and "ticker" not in cols:
            rename["symbol"] = "ticker"

        # week detection
        week_candidates = ["week", "week_start", "week_begin", "week_start_date", "date", "trade_date", "period", "weekdate"]
        for c in week_candidates:
            if c in cols:
                rename[c] = "week"
                break

        # predicted score
        if is_pred:
            if "predicted_score" not in cols:
                if "score" in cols:
                    rename["score"] = "predicted_score"
                elif "predicted_rank" in cols:
                    rename["predicted_rank"] = "predicted_score"

        # realized return
        if is_real:
            if "realized_return" not in cols:
                if "realized_week_gain" in cols:
                    rename["realized_week_gain"] = "realized_return"
                elif "realized_week_return" in cols:
                    rename["realized_week_return"] = "realized_return"
                elif "return" in cols:
                    rename["return"] = "realized_return"

        if rename:
            df = df.rename(columns=rename)

        # coerce types
        if "week" in df.columns:
            df["week"] = pd.to_datetime(df["week"], errors="coerce")
        if "ticker" in df.columns:
            df["ticker"] = df["ticker"].astype(str)
        return df

    pred_df = map_cols(pred_df, is_pred=True, is_real=False)
    real_df = map_cols(real_df, is_pred=False, is_real=True)
    return pred_df, real_df


def safe_merge(pred, real):
    pred = standardize_columns(pred)
    real = standardize_columns(real)

    pred, real = detect_and_normalize_columns(pred, real)

    # drop rows missing week or ticker where those columns exist
    if "week" in pred.columns and "ticker" in pred.columns:
        pred = pred.dropna(subset=["week", "ticker"])
    if "week" in real.columns and "ticker" in real.columns:
        real = real.dropna(subset=["week", "ticker"])

    # ensure both have 'week' and 'ticker' columns for merge; if not, raise informative error
    missing = []
    if "week" not in pred.columns and "week" not in real.columns:
        missing.append("Neither predicted nor realized files contain a week-like column.")
    if "ticker" not in pred.columns and "ticker" not in real.columns:
        missing.append("Neither predicted nor realized files contain a ticker/symbol column.")
    if missing:
        raise RuntimeError("Column detection failed: " + " ".join(missing))

    merged = pd.merge(pred, real, on=["week", "ticker"], how="outer", suffixes=("_pred", "_real"))
    return pred, real, merged


def compute_full_metrics(merged):
    if "realized_return" not in merged.columns:
        return None

    df = merged.dropna(subset=["realized_return"])
    if df.empty:
        return None

    df = df.sort_values("week")
    realized = pd.to_numeric(df["realized_return"], errors="coerce").dropna()

    mean_weekly = float(realized.mean())
    std_weekly = float(realized.std(ddof=0)) if len(realized) > 1 else float("nan")
    hit_rate = float((realized > 0).mean())
    ir_weekly = float(mean_weekly / std_weekly) if std_weekly and not np.isnan(std_weekly) else float("nan")
    ir_annual = float(ir_weekly * np.sqrt(52)) if not np.isnan(ir_weekly) else float("nan")

    pred_col = None
    for c in ["predicted_score", "score_pred", "score", "predicted_rank", "predicted_weight"]:
        if c in df.columns:
            pred_col = c
            break

    pearson = float(df[pred_col].corr(df["realized_return"])) if pred_col is not None else float("nan")
    try:
        if pred_col is not None:
            sp = spearmanr(df[pred_col].rank(method="average"), df["realized_return"], nan_policy="omit")
            spearman_corr = float(sp.correlation) if sp and hasattr(sp, "correlation") else float("nan")
        else:
            spearman_corr = float("nan")
    except Exception:
        spearman_corr = float("nan")

    weekly = df.groupby("week")["realized_return"].mean().sort_index()
    weekly = pd.to_numeric(weekly, errors="coerce").dropna()
    cumulative = (1 + weekly).cumprod() - 1

    picks_by_week = merged.dropna(subset=["ticker"]).groupby("week")["ticker"].apply(lambda s: set(s.dropna().astype(str)))
    weeks = sorted(picks_by_week.index)
    turnover_list = []
    prev = set()
    for w in weeks:
        cur = picks_by_week[w]
        if not prev:
            turnover_list.append((w, 0.0))
        else:
            new_frac = len(cur - prev) / max(1, len(prev))
            turnover_list.append((w, float(new_frac)))
        prev = cur
    turnover_df = pd.DataFrame(turnover_list, columns=["week", "turnover_new_frac"]).set_index("week")

    return {
        "full": {
            "mean_weekly_return": mean_weekly,
            "std_weekly_return": std_weekly,
            "hit_rate": hit_rate,
            "ir_weekly": ir_weekly,
            "ir_annual": ir_annual,
            "pearson_corr": pearson,
            "spearman_corr": spearman_corr,
            "n_picks": int(len(df)),
            "n_weeks": int(df["week"].nunique()),
            "n_tickers": int(df["ticker"].nunique()),
        },
        "weekly_series": weekly,
        "cumulative": cumulative,
        "turnover": turnover_df,
    }


def compute_rolling_metrics(merged, windows=(4, 12, 52)):
    merged = merged.sort_values("week")
    if "realized_return" not in merged.columns:
        return {}
    merged = merged.dropna(subset=["realized_return"])
    if merged.empty:
        return {}
    out = {}
    weekly = merged.groupby("week")["realized_return"].mean().sort_index()
    weekly = pd.to_numeric(weekly, errors="coerce").dropna()
    for w in windows:
        roll = weekly.rolling(window=w, min_periods=1).agg(["mean", "std"])
        roll["ir_weekly"] = roll["mean"] / roll["std"]
        out[f"rolling_{w}w"] = roll
    return out


def per_ticker_diagnostics(merged):
    if "realized_return" not in merged.columns:
        return pd.DataFrame()
    df = merged.dropna(subset=["realized_return"])
    if df.empty:
        return pd.DataFrame()
    g = df.groupby("ticker")["realized_return"]
    out = pd.DataFrame({
        "ticker": g.apply(lambda s: s.name),
        "total_picks": g.count(),
        "mean_realized_return": g.mean(),
        "std_realized_return": g.std(ddof=0),
        "hit_rate": g.apply(lambda s: (s > 0).mean()),
        "first_picked_week": df.groupby("ticker")["week"].min(),
        "last_picked_week": df.groupby("ticker")["week"].max(),
    }).reset_index(drop=True)
    cols = ["ticker", "total_picks", "mean_realized_return", "std_realized_return", "hit_rate", "first_picked_week", "last_picked_week"]
    out = out[cols]
    return out


def locate_file(primary_path, name_hint):
    if os.path.exists(primary_path):
        return primary_path
    patterns = [f"*{name_hint}*.csv", f"*{name_hint}*.parquet"]
    fb = find_fallback(patterns)
    return fb


def process_strategy(name, pred_path, real_path):
    report = {"strategy": name, "pred_file": None, "real_file": None, "status": "missing_both"}
    pred_file = locate_file(pred_path, "predicted")
    real_file = locate_file(real_path, "realized")
    report["pred_file"] = pred_file
    report["real_file"] = real_file

    if not pred_file and not real_file:
        report["status"] = "missing_both"
        return report

    pred_df = load_csv(pred_file) if pred_file else pd.DataFrame()
    real_df = load_csv(real_file) if real_file else pd.DataFrame()

    pred_df = standardize_columns(pred_df)
    real_df = standardize_columns(real_df)

    pred_df, real_df = detect_and_normalize_columns(pred_df, real_df)

    if "week" in pred_df.columns and "ticker" in pred_df.columns:
        pred_df = pred_df.dropna(subset=["week", "ticker"])
    if "week" in real_df.columns and "ticker" in real_df.columns:
        real_df = real_df.dropna(subset=["week", "ticker"])

    # tolerant placeholder for empty predicted file
    if (pred_df is None) or pred_df.empty:
        pred_df = pd.DataFrame(columns=["week", "ticker", "predicted_score"])
        if "week" in real_df.columns:
            pred_df["week"] = pd.to_datetime([], errors="coerce")
        if "ticker" in real_df.columns:
            pred_df["ticker"] = pd.Series(dtype=str)

    try:
        pred_df, real_df, merged = safe_merge(pred_df, real_df)
    except Exception as e:
        report["status"] = "failed_merge"
        report["error"] = str(e)
        return report

    total_weeks = int(merged["week"].nunique()) if "week" in merged.columns else 0
    total_tickers = int(pd.concat([
        pred_df["ticker"] if "ticker" in pred_df.columns else pd.Series(dtype=str),
        real_df["ticker"] if "ticker" in real_df.columns else pd.Series(dtype=str)
    ]).nunique())

    matched = 0
    if "realized_return" in merged.columns:
        matched = int(merged.dropna(subset=["realized_return"]).shape[0])

    unmatched_pred = 0
    unmatched_real = 0
    if not pred_df.empty and not real_df.empty:
        unmatched_pred = int(pred_df.merge(real_df[["week", "ticker"]], on=["week", "ticker"], how="left", indicator=True).query("_merge=='left_only'").shape[0])
        unmatched_real = int(real_df.merge(pred_df[["week", "ticker"]], on=["week", "ticker"], how="left", indicator=True).query("_merge=='left_only'").shape[0])
    else:
        unmatched_pred = int(pred_df.shape[0]) if not pred_df.empty else 0
        unmatched_real = int(real_df.shape[0]) if not real_df.empty else 0

    metrics = compute_full_metrics(merged)
    rolling = compute_rolling_metrics(merged)
    diag = per_ticker_diagnostics(merged)

    summary_csv = os.path.join(ANALYSIS_DIR, f"verification_summary_{name}.csv")
    diag_csv = os.path.join(ANALYSIS_DIR, f"verification_diagnostics_{name}.csv")
    timeseries_png = os.path.join(ANALYSIS_DIR, f"verification_time_series_{name}.png")

    summary = {
        "strategy": name,
        "pred_file": pred_file,
        "real_file": real_file,
        "total_weeks": total_weeks,
        "total_tickers": total_tickers,
        "matched_rows": matched,
        "unmatched_pred_rows": unmatched_pred,
        "unmatched_real_rows": unmatched_real,
    }

    if metrics:
        summary.update(metrics["full"])
    else:
        summary.update({
            "mean_weekly_return": float("nan"),
            "std_weekly_return": float("nan"),
            "hit_rate": float("nan"),
            "ir_weekly": float("nan"),
            "ir_annual": float("nan"),
            "pearson_corr": float("nan"),
            "spearman_corr": float("nan"),
            "n_picks": 0
        })

    pd.DataFrame([summary]).to_csv(summary_csv, index=False)
    diag.to_csv(diag_csv, index=False)

    plt.figure(figsize=(10, 6))
    plotted = False
    if metrics and metrics.get("cumulative") is not None and not metrics["cumulative"].empty:
        metrics["cumulative"].plot(label="Cumulative (eq-wtd)")
        plotted = True
    if "rolling_12w" in rolling and not rolling["rolling_12w"].empty:
        rolling["rolling_12w"]["ir_weekly"].plot(label="Rolling IR 12w", style="--", secondary_y=True)
        plotted = True
    plt.title(f"Verification time series: {name}")
    if plotted:
        plt.legend(loc="upper left")
        plt.tight_layout()
        plt.savefig(timeseries_png)
        plt.close()
    else:
        plt.figure(figsize=(6, 2))
        plt.text(0.5, 0.5, f"No matched realized rows to plot for {name}", ha="center", va="center")
        plt.axis("off")
        plt.savefig(timeseries_png)
        plt.close()

    report.update({
        "status": "ok",
        "summary_csv": summary_csv,
        "diag_csv": diag_csv,
        "timeseries_png": timeseries_png,
    })
    return report


def main():
    reports = []
    artifacts = []
    for strat in ["longonly", "longshort"]:
        r = process_strategy(strat, EXPECTED[strat]["pred"], EXPECTED[strat]["real"])
        reports.append(r)
        for k in ["summary_csv", "diag_csv", "timeseries_png"]:
            if k in r and r[k]:
                artifacts.append(r[k])

    run_report_path = os.path.join(ANALYSIS_DIR, "verification_run_report.txt")
    with open(run_report_path, "w", encoding="utf8") as f:
        f.write(f"Verification run at {datetime.now(timezone.utc).isoformat()}Z\n\n")
        for r in reports:
            f.write(json.dumps(r, default=str) + "\n")
    artifacts.append(run_report_path)

    print("Verification run complete.")
    for r in reports:
        print(f"Strategy: {r.get('strategy')}, status: {r.get('status')}")
        if r.get("status") == "ok":
            print(f"  Summary CSV: {r.get('summary_csv')}")
            print(f"  Diagnostics CSV: {r.get('diag_csv')}")
            print(f"  Time series PNG: {r.get('timeseries_png')}")
        else:
            print(f"  Predicted file: {r.get('pred_file')}")
            print(f"  Realized file: {r.get('real_file')}")
            if "error" in r:
                print(f"  Error: {r['error']}")

    print("\nArtifacts saved to:", ANALYSIS_DIR)

    out = {
        "status": "partial" if any(r.get("status") != "ok" for r in reports) else "success",
        "longonly_files_found": [reports[0].get("pred_file"), reports[0].get("real_file")],
        "longshort_files_found": [reports[1].get("pred_file"), reports[1].get("real_file")],
        "artifacts": artifacts,
        "message": "See verification_run_report.txt in analysis for details."
    }
    print("\nJSON_SUMMARY:")
    print(json.dumps(out, default=str))
    return out


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("Verification run failed with exception:", str(exc))
        sys.exit(2)
