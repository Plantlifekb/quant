#!/usr/bin/env python3
"""regen_and_verify.py - docstring removed to avoid unicodeescape errors"""
import os
import json
from datetime import timedelta
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import spearmanr

ROOT = r"C:\Quant"
ANALYSIS = os.path.join(ROOT, "analysis")
os.makedirs(ANALYSIS, exist_ok=True)

INGEST = os.path.join(ROOT, "data", "ingestion", "ingestion_5years.csv")
PRICES_PARQ = os.path.join(ROOT, "data", "ingestion", "prices.parquet")

PICK_CANDIDATES = {
    "longonly": [
        os.path.join(ROOT, "outputs", "verification", "predicted_vs_picks_weekly_longonly.csv"),
        os.path.join(ROOT, "data", "signals", "weekly_selection.csv"),
        os.path.join(ROOT, "data", "signals", "weekly_selection_canonical_prepped.csv"),
    ],
    "longshort": [
        os.path.join(ROOT, "outputs", "verification", "predicted_vs_picks_weekly_longshort.csv"),
        os.path.join(ROOT, "data", "signals", "weekly_selection_longshort.csv"),
        os.path.join(ROOT, "data", "signals", "weekly_selection_canonical_prepped.csv"),
    ],
}

OUT = {
    "regen_longonly": os.path.join(ANALYSIS, "regenerated_realized_longonly.csv"),
    "regen_longshort": os.path.join(ANALYSIS, "regenerated_realized_longshort.csv"),
    "summary_longonly": os.path.join(ANALYSIS, "regen_verification_summary_longonly.csv"),
    "summary_longshort": os.path.join(ANALYSIS, "regen_verification_summary_longshort.csv"),
    "diag_longonly": os.path.join(ANALYSIS, "regen_verification_diagnostics_longonly.csv"),
    "diag_longshort": os.path.join(ANALYSIS, "regen_verification_diagnostics_longshort.csv"),
    "png_longonly": os.path.join(ANALYSIS, "regen_time_series_longonly.png"),
    "png_longshort": os.path.join(ANALYSIS, "regen_time_series_longshort.png"),
    "run_report": os.path.join(ANALYSIS, "regen_run_report.txt"),
    "realized_weekly": os.path.join(ANALYSIS, "realized_weekly_from_ingest.csv"),
}


def find_first_existing(paths):
    for p in paths:
        if os.path.exists(p):
            return p
    return None


def load_csv(path, **kwargs):
    return pd.read_csv(path, **kwargs)


def preview_ingest(path, n=5):
    try:
        df = load_csv(path, nrows=n)
        return list(df.columns), df.head(n).to_dict(orient="records")
    except Exception:
        return [], []


def build_weekly_realized_dayfirst():
    # load source
    if os.path.exists(INGEST):
        df = load_csv(INGEST, low_memory=False)
    elif os.path.exists(PRICES_PARQ):
        df = pd.read_parquet(PRICES_PARQ)
    else:
        raise FileNotFoundError("No ingestion source found (ingestion_5years.csv or prices.parquet).")

    df.columns = [c.strip().lower() for c in df.columns]

    # detect date, ticker, price columns
    date_col = next((c for c in df.columns if c in ("date", "run_date", "trade_date", "timestamp")), None)
    ticker_col = next((c for c in df.columns if c in ("ticker", "symbol", "sid")), None)
    price_col = 'adj_close' if 'adj_close' in df.columns and df['adj_close'].notna().any() else ('close' if 'close' in df.columns and df['close'].notna().any() else None)

    # if ingestion already contains weekly realized returns, use them
    for cand in ("realized_return", "realized_week_gain", "realized_week_return", "weekly_return"):
        if cand in df.columns and (date_col or "week" in df.columns):
            out = df.rename(columns={cand: "realized_return"})
            if "week" not in out.columns and date_col:
                out = out.rename(columns={date_col: "week"})
            if "ticker" not in out.columns and ticker_col:
                out = out.rename(columns={ticker_col: "ticker"})
            if "week" in out.columns and "ticker" in out.columns:
                out["week"] = pd.to_datetime(out["week"], errors="coerce", dayfirst=True).dt.normalize()
                out["ticker"] = out["ticker"].astype(str)
                out = out.dropna(subset=["week", "ticker"])
                return out[["week", "ticker", "realized_return"]]

    # require date, ticker, price to compute weekly returns
    if date_col is None or ticker_col is None or price_col is None:
        raise RuntimeError(f"ingestion missing required columns. Found columns: {list(df.columns)[:40]}")

    # keep only needed columns and coerce types
    df = df[[date_col, ticker_col, price_col]].dropna(subset=[date_col, ticker_col])
    df = df.rename(columns={date_col: "date", ticker_col: "ticker", price_col: "close"})
    # parse dates with dayfirst=True to handle DD/MM/YYYY
    df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["date"])
    df["ticker"] = df["ticker"].astype(str)

    weekly_frames = []
    for t, g in df.groupby("ticker"):
        g = g.sort_values("date").set_index("date")
        try:
            first = g["close"].resample("W-MON").first()
            last = g["close"].resample("W-MON").last()
            w = pd.DataFrame({"close_start": first, "close_end": last}).dropna()
            if w.empty:
                continue
            w["realized_return"] = w["close_end"] / w["close_start"] - 1.0
            w = w.reset_index().rename(columns={"index": "week"})
            w["ticker"] = t
            weekly_frames.append(w[["week", "ticker", "realized_return"]])
        except Exception:
            continue

    if not weekly_frames:
        raise RuntimeError("No weekly returns could be computed from ingestion file.")

    realized = pd.concat(weekly_frames, ignore_index=True)
    realized["week"] = pd.to_datetime(realized["week"]).dt.normalize()
    realized["ticker"] = realized["ticker"].astype(str)
    realized.to_csv(OUT["realized_weekly"], index=False)
    return realized


def load_picks(path):
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    df = load_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]
    if "week_start" in df.columns and "week" not in df.columns:
        df = df.rename(columns={"week_start": "week"})
    if "symbol" in df.columns and "ticker" not in df.columns:
        df = df.rename(columns={"symbol": "ticker"})
    if "score" in df.columns and "predicted_score" not in df.columns:
        df = df.rename(columns={"score": "predicted_score"})
    if "week" in df.columns:
        df["week"] = pd.to_datetime(df["week"], errors="coerce", dayfirst=True).dt.normalize()
    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str)
    return df



def safe_match(left, realized):
    import pandas as pd
    # defensive copy
    m2 = realized.copy()
    # normalize week column if present
    if "week" in m2.columns:
        m2["week"] = pd.to_datetime(m2["week"], errors="coerce").dt.normalize()
    # find a realized-return column or compute it from close_end/close_start
    rr_col = next((c for c in ["realized_return", "realized", "return", "ret"] if c in m2.columns), None)
    if rr_col is None:
        if "close_end" in m2.columns and "close_start" in m2.columns:
            m2["realized_return"] = m2["close_end"] / m2["close_start"] - 1.0
            rr_col = "realized_return"
        else:
            raise KeyError("realized_return column missing in realized frame")
    # index realized by week[, ticker]
    if "ticker" in m2.columns:
        m2 = m2.set_index(["week", "ticker"])[rr_col]
    else:
        m2 = m2.set_index("week")[rr_col]
    # prepare left (picks) and normalize week
    left2 = left.copy()
    if "week" in left2.columns:
        left2["week"] = pd.to_datetime(left2["week"], errors="coerce").dt.normalize()
    # lookup function that tolerates missing keys
    def _lookup(r):
        try:
            if "ticker" in left2.columns and isinstance(m2.index, pd.MultiIndex):
                key = (r.get("week"), str(r.get("ticker")))
            else:
                key = r.get("week")
            return m2.loc[key]
        except Exception:
            return None
    # apply and return
    left2["realized_return"] = left2.apply(_lookup, axis=1)
    return left2
def compute_and_save(strategy, picks_matched):
    picks_matched.to_csv(OUT["regen_" + strategy], index=False)
    matched = picks_matched.dropna(subset=["realized_return"]).copy()
    summary = {"strategy": strategy, "input_rows": int(len(picks_matched)), "matched_rows": int(len(matched))}
    if matched.empty:
        pd.DataFrame([summary]).to_csv(OUT["summary_" + strategy], index=False)
        pd.DataFrame().to_csv(OUT["diag_" + strategy], index=False)
        plt.figure(figsize=(6, 2)); plt.text(0.5, 0.5, f"No matched realized rows for {strategy}", ha="center"); plt.axis("off"); plt.savefig(OUT["png_" + strategy]); plt.close()
        summary["status"] = "no_matches"
        return summary
    realized = matched["realized_return"].astype(float)
    mean_weekly = float(realized.mean())
    std_weekly = float(realized.std(ddof=0)) if len(realized) > 1 else float("nan")
    hit_rate = float((realized > 0).mean())
    ir_weekly = float(mean_weekly / std_weekly) if std_weekly and not np.isnan(std_weekly) else float("nan")
    ir_annual = float(ir_weekly * (52 ** 0.5)) if not np.isnan(ir_weekly) else float("nan")
    pred_col = next((c for c in ("predicted_score", "score", "predicted_rank") if c in matched.columns), None)
    pearson = float(matched[pred_col].corr(matched["realized_return"])) if pred_col else float("nan")
    try:
        spearman_corr = float(spearmanr(matched[pred_col].rank(method="average"), matched["realized_return"], nan_policy="omit").correlation) if pred_col else float("nan")
    except Exception:
        spearman_corr = float("nan")
    weekly_port = matched.groupby("week")["realized_return"].mean().sort_index()
    cumulative = (1 + weekly_port).cumprod() - 1
    g = matched.groupby("ticker")["realized_return"]
    diag = pd.DataFrame({
        "ticker": g.apply(lambda s: s.name),
        "total_picks": g.count(),
        "mean_realized_return": g.mean(),
        "std_realized_return": g.std(ddof=0),
        "hit_rate": g.apply(lambda s: (s > 0).mean()),
        "first_picked_week": matched.groupby("ticker")["week"].min(),
        "last_picked_week": matched.groupby("ticker")["week"].max()
    }).reset_index(drop=True)
    pd.DataFrame([{
        "strategy": strategy,
        "mean_weekly_return": mean_weekly,
        "std_weekly_return": std_weekly,
        "hit_rate": hit_rate,
        "ir_weekly": ir_weekly,
        "ir_annual": ir_annual,
        "pearson_corr": pearson,
        "spearman_corr": spearman_corr,
        "n_picks": int(len(matched)),
        "n_weeks": int(weekly_port.shape[0]),
        "n_tickers": int(diag.shape[0])
    }]).to_csv(OUT["summary_" + strategy], index=False)
    diag.to_csv(OUT["diag_" + strategy], index=False)
    plt.figure(figsize=(10, 6)); ax = plt.gca(); cumulative.plot(ax=ax, label="Cumulative (eq-wtd)"); ax.set_title(f"Regenerated verification: {strategy}"); ax.legend(); plt.tight_layout(); plt.savefig(OUT["png_" + strategy]); plt.close()
    return {"strategy": strategy, "status": "ok", "matched_rows": int(len(matched))}


def main():
    run_report = {"issues": [], "runs": []}
    artifacts = []
    try:
        realized = build_weekly_realized_dayfirst()
    except Exception as e:
        msg = f"weekly build failed: {e}"
        run_report["issues"].append(msg)
        with open(OUT["run_report"], "w", encoding="utf8") as f:
            f.write(msg + "\n\n")
            if os.path.exists(INGEST):
                cols, sample = preview_ingest(INGEST, n=3)
                f.write("ingestion_5years.csv columns:\n" + ", ".join(cols) + "\n\n")
                f.write("sample rows:\n" + json.dumps(sample, default=str) + "\n\n")
        print(json.dumps({"status": "failed", "artifacts": [], "issues": run_report["issues"], "message": msg}))
        return
    for strat in ("longonly", "longshort"):
        pick_path = find_first_existing(PICK_CANDIDATES[strat])
        picks = pd.DataFrame() if not pick_path else load_csv(pick_path)
        if picks.empty:
            run_report["issues"].append(f"No pick file found for {strat}; looked at: {PICK_CANDIDATES[strat]}")
            pd.DataFrame().to_csv(OUT["regen_" + strat], index=False)
            pd.DataFrame([{"strategy": strat, "status": "no_picks"}]).to_csv(OUT["summary_" + strat], index=False)
            run_report["runs"].append({"strategy": strat, "status": "no_picks"})
            continue
        picks.columns = [c.strip().lower() for c in picks.columns]
        if "week_start" in picks.columns and "week" not in picks.columns:
            picks = picks.rename(columns={"week_start": "week"})
        if "symbol" in picks.columns and "ticker" not in picks.columns:
            picks = picks.rename(columns={"symbol": "ticker"})
        if "score" in picks.columns and "predicted_score" not in picks.columns:
            picks = picks.rename(columns={"score": "predicted_score"})
        if "week" in picks.columns:
            picks["week"] = pd.to_datetime(picks["week"], errors="coerce", dayfirst=True).dt.normalize()
        if "ticker" in picks.columns:
            picks["ticker"] = picks["ticker"].astype(str)
        if "week" not in picks.columns or "ticker" not in picks.columns:
            run_report["issues"].append(f"Picks for {strat} missing week or ticker columns in {pick_path}")
            pd.DataFrame().to_csv(OUT["regen_" + strat], index=False)
            pd.DataFrame([{"strategy": strat, "status": "missing_keys"}]).to_csv(OUT["summary_" + strat], index=False)
            run_report["runs"].append({"strategy": strat, "status": "missing_keys"})
            continue
        picks_matched = safe_match(picks, realized)
        res = compute_and_save(strat, picks_matched)
        run_report["runs"].append(res)
        artifacts.extend([OUT["regen_" + strat], OUT["summary_" + strat], OUT["diag_" + strat], OUT["png_" + strat]])
    with open(OUT["run_report"], "w", encoding="utf8") as f:
        f.write(json.dumps(run_report, default=str, indent=2))
    artifacts.append(OUT["run_report"])
    print(json.dumps({"status": "success", "artifacts": artifacts, "issues": run_report["issues"], "message": "regeneration complete"}))


if __name__ == "__main__":
    main()

