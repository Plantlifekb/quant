import os
import sys
import numpy as np
import pandas as pd

try:
    from logging_quant_v1 import get_logger
except ImportError:
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    def get_logger(name: str):
        return logging.getLogger(name)

LOGGER = get_logger("quant_weekly_10x10_v1")

INGESTION_PATH = r"C:\Quant\data\ingestion\ingestion_5years.csv"

# *** KEY CHANGE: use the historical ensemble+regime file ***
WEIGHTS_PATH = r"C:\Quant\data\analytics\quant_portfolio_weights_regime_history_v1.csv"

PICKS_OUT = r"C:\Quant\data\analytics\quant_weekly_10x10_picks_v1.csv"
PERF_OUT  = r"C:\Quant\data\analytics\quant_weekly_10x10_perf_v1.csv"


def load_ingestion():
    df = pd.read_csv(INGESTION_PATH)
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df["ticker"] = df["ticker"].str.upper()
    return df


def load_weights():
    df = pd.read_csv(WEIGHTS_PATH)
    df.columns = [c.lower() for c in df.columns]

    # historical file already has week_start, but we keep date logic intact
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], utc=True)
    if "week_start" in df.columns:
        df["week_start"] = pd.to_datetime(df["week_start"], utc=True)

    df["ticker"] = df["ticker"].str.upper()
    if "regime_label" not in df.columns:
        df["regime_label"] = "unknown"
    return df


def add_week_start(df, col="date"):
    dt = pd.to_datetime(df[col], utc=True)
    df["week_start"] = (dt - pd.to_timedelta(dt.dt.weekday, unit="D")).dt.normalize()
    return df


def compute_weekly_returns(ing):
    df = ing[["date", "ticker", "close"]].copy()
    df = add_week_start(df, "date")
    df = df.sort_values(["ticker", "date"])
    df["daily_return"] = df.groupby("ticker")["close"].pct_change()
    df = df.dropna(subset=["daily_return"])
    weekly = (
        df.groupby(["week_start", "ticker"], as_index=False)["daily_return"]
        .apply(lambda x: (1 + x).prod() - 1)
        .rename(columns={"daily_return": "realized_weekly_return"})
    )
    return weekly


def select_week(ws, w_week, r_week):
    merged = pd.merge(w_week, r_week, on=["week_start", "ticker"], how="inner")
    if merged.empty:
        return pd.DataFrame(), pd.DataFrame()

    longs = merged[merged["weight"] > 0].sort_values("expected_return", ascending=False)
    shorts = merged[merged["weight"] < 0].sort_values("expected_return", ascending=True)

    if len(longs) < 10:
        return pd.DataFrame(), pd.DataFrame()

    # Long-only
    lo = longs.head(10).copy()
    lo["side"] = "long"
    lo["rank"] = np.arange(1, 11)
    lo["strategy"] = "long_only"
    lo["date"] = ws
    lo["weight"] = 0.1
    lo["portfolio_weekly_return"] = lo["realized_weekly_return"].mean()

    picks_lo = lo[
        [
            "week_start", "date", "strategy", "side", "rank",
            "ticker", "weight", "expected_return",
            "realized_weekly_return", "portfolio_weekly_return",
            "regime_label",
        ]
    ].copy()

    # Long-short
    if len(shorts) < 10:
        return picks_lo, pd.DataFrame()

    ls_long = longs.head(10).copy()
    ls_short = shorts.head(10).copy()

    ls_long["side"] = "long"
    ls_short["side"] = "short"
    ls_long["rank"] = np.arange(1, 11)
    ls_short["rank"] = np.arange(1, 11)
    ls_long["strategy"] = "long_short"
    ls_short["strategy"] = "long_short"
    ls_long["date"] = ws
    ls_short["date"] = ws
    ls_long["weight"] = 0.1
    ls_short["weight"] = -0.1

    long_ret = ls_long["realized_weekly_return"].mean()
    short_ret = ls_short["realized_weekly_return"].mean()
    port_ret = long_ret - short_ret

    ls_long["portfolio_weekly_return"] = port_ret
    ls_short["portfolio_weekly_return"] = port_ret

    picks_ls = pd.concat([ls_long, ls_short], ignore_index=True)[
        [
            "week_start", "date", "strategy", "side", "rank",
            "ticker", "weight", "expected_return",
            "realized_weekly_return", "portfolio_weekly_return",
            "regime_label",
        ]
    ].copy()

    return picks_lo, picks_ls


def build_perf(picks):
    perf = (
        picks.groupby(["week_start", "strategy"], as_index=False)["portfolio_weekly_return"]
        .first()
        .rename(columns={"portfolio_weekly_return": "weekly_return"})
    )
    perf = perf.sort_values(["strategy", "week_start"])

    def add_curves(g):
        r = g["weekly_return"].values
        cum = np.cumprod(1 + r) - 1
        peak = np.maximum.accumulate(1 + cum)
        dd = (1 + cum) / peak - 1
        g["cumulative_return"] = cum
        g["drawdown"] = dd
        return g

    perf = perf.groupby("strategy", group_keys=False).apply(add_curves)
    return perf


def main():
    LOGGER.info("Start weekly engine")

    ing = load_ingestion()
    w = load_weights()
    r = compute_weekly_returns(ing)

    # Ensure week_start exists on weights
    if "week_start" not in w.columns:
        w = add_week_start(w, "date")

    weeks = sorted(set(w["week_start"]) & set(r["week_start"]))

    picks_all = []

    for ws in weeks:
        ws = pd.to_datetime(ws, utc=True)
        w_week = w[w["week_start"] == ws]
        r_week = r[r["week_start"] == ws]
        lo, ls = select_week(ws, w_week, r_week)
        if not lo.empty:
            picks_all.append(lo)
        if not ls.empty:
            picks_all.append(ls)

    if not picks_all:
        LOGGER.error("No picks generated")
        sys.exit(1)

    picks = pd.concat(picks_all, ignore_index=True)
    picks.to_csv(PICKS_OUT, index=False)
    LOGGER.info("Wrote picks")

    perf = build_perf(picks)
    perf.to_csv(PERF_OUT, index=False)
    LOGGER.info("Wrote performance")

    LOGGER.info("Done")


if __name__ == "__main__":
    main()
