import sys
import re
from pathlib import Path

import pandas as pd

BASE = Path(r"C:\Quant")

DATA_INGESTION = BASE / "data" / "ingestion"
DATA_SIGNALS = BASE / "data" / "signals"
DATA_ANALYTICS = BASE / "data" / "analytics"
DATA_REFERENCE = BASE / "data" / "reference"
SCRIPTS_DASHBOARD = BASE / "scripts" / "dashboard"

REQUIRED_FILES = {
    "prices": DATA_INGESTION / "prices.parquet",
    "fundamentals": DATA_INGESTION / "fundamentals.parquet",
    "risk_model": DATA_INGESTION / "risk_model.parquet",
    "securities_master": DATA_REFERENCE / "securities_master.parquet",
    "expected_returns": DATA_SIGNALS / "expected_returns_quant_v1.parquet",
    "optimiser": DATA_ANALYTICS / "optimiser_regime_quant_v1.parquet",
    "portfolio_performance": DATA_ANALYTICS / "portfolio_performance_quant_v1.parquet",
    "factor_exposure_report": DATA_ANALYTICS / "factor_exposure_report_quant_v1.parquet",
    "turnover_regime": DATA_ANALYTICS / "turnover_regime_quant_v1.parquet",
}


def fail(msg: str) -> None:
    print(f"\n❌ FAIL: {msg}\n")
    sys.exit(1)


def ok(msg: str) -> None:
    print(f"✔ {msg}")


def latest_date(df: pd.DataFrame, col: str):
    return pd.to_datetime(df[col]).max().date()


def verify_file_existence() -> None:
    print("\n--- 1. Checking required governed files exist ---\n")
    for name, path in REQUIRED_FILES.items():
        if not path.exists():
            fail(f"Missing required file: {path}")
        ok(f"Found {name}: {path}")


def verify_ingestion(prices: pd.DataFrame, securities_master: pd.DataFrame) -> None:
    print("\n--- 2. Verifying ingestion layer contracts ---\n")

    required_cols = ["date", "ticker", "price", "return"]
    missing = [c for c in required_cols if c not in prices.columns]
    if missing:
        fail(f"prices.parquet missing required columns: {missing}")

    if prices.duplicated(subset=["date", "ticker"]).any():
        fail("prices.parquet contains duplicate (date, ticker) rows")

    for c in required_cols:
        if prices[c].isna().any():
            fail(f"prices.parquet contains NaNs in required column: {c}")
    ok("prices.parquet schema and basic invariants satisfied")

    if "ticker" not in securities_master.columns:
        fail("securities_master.parquet missing 'ticker' column")

    price_tickers = set(prices["ticker"].unique())
    universe_tickers = set(securities_master["ticker"].unique())
    missing_in_master = price_tickers - universe_tickers
    if missing_in_master:
        fail(
            "Tickers in prices.parquet not found in securities_master.parquet: "
            f"{sorted(list(missing_in_master))[:10]}..."
        )
    ok("All tickers in prices.parquet are present in securities_master.parquet")
    ok("Ingestion layer checks passed")


def verify_signals(exp: pd.DataFrame, prices: pd.DataFrame) -> None:
    print("\n--- 3. Verifying signal layer contracts ---\n")

    required_cols = ["date", "ticker", "expected_return", "regime", "signal_version"]
    missing = [c for c in required_cols if c not in exp.columns]
    if missing:
        fail(f"expected_returns_quant_v1.parquet missing required columns: {missing}")

    if exp["expected_return"].isna().any():
        fail("expected_returns_quant_v1.parquet contains NaNs in expected_return")
    ok("Signal file schema and expected_return completeness satisfied")

    latest_prices_date = latest_date(prices, "date")
    latest_exp_date = latest_date(exp, "date")

    if latest_prices_date == latest_exp_date:
        p_tickers = set(
            prices.loc[pd.to_datetime(prices["date"]).dt.date == latest_prices_date, "ticker"]
        )
        e_tickers = set(
            exp.loc[pd.to_datetime(exp["date"]).dt.date == latest_exp_date, "ticker"]
        )
        missing_in_exp = p_tickers - e_tickers
        if missing_in_exp:
            fail(
                f"Signal layer missing tickers for latest date {latest_exp_date}: "
                f"{sorted(list(missing_in_exp))[:10]}..."
            )
        ok("Signal layer has full universe coverage for latest date")

    ok("Signal layer checks passed")


def verify_optimiser(opt: pd.DataFrame, securities_master: pd.DataFrame) -> None:
    print("\n--- 4. Verifying optimiser layer contracts ---\n")

    required_cols = [
        "date",
        "ticker",
        "weight",
        "regime",
        "gross_leverage_target",
        "net_exposure_min",
        "net_exposure_max",
        "lambda_risk",
        "gamma_turnover",
    ]
    missing = [c for c in required_cols if c not in opt.columns]
    if missing:
        fail(f"optimiser_regime_quant_v1.parquet missing required columns: {missing}")
    ok("Optimiser file has required core columns")

    if opt["weight"].isna().any():
        fail("optimiser_regime_quant_v1.parquet contains NaNs in weight")

    opt["date"] = pd.to_datetime(opt["date"])
    latest_opt_date = opt["date"].max()
    latest_group = opt[opt["date"] == latest_opt_date]

    gross = latest_group["weight"].abs().sum()
    target = latest_group["gross_leverage_target"].iloc[0]
    if gross > target + 0.05:
        fail(
            f"Gross leverage {gross:.3f} exceeds target {target:.3f} on {latest_opt_date.date()}"
        )

    net = latest_group["weight"].sum()
    net_min = latest_group["net_exposure_min"].iloc[0]
    net_max = latest_group["net_exposure_max"].iloc[0]
    if not (net_min - 0.02 <= net <= net_max + 0.02):
        fail(
            f"Net exposure {net:.3f} outside [{net_min:.3f}, {net_max:.3f}] "
            f"on {latest_opt_date.date()}"
        )

    ok("Optimiser leverage and net exposure checks passed")

    if "ticker" not in securities_master.columns:
        fail("securities_master.parquet missing 'ticker' column")

    opt_tickers = set(opt["ticker"].unique())
    universe_tickers = set(securities_master["ticker"].unique())
    missing_in_master = opt_tickers - universe_tickers
    if missing_in_master:
        fail(
            "Tickers in optimiser_regime_quant_v1.parquet not found in securities_master.parquet: "
            f"{sorted(list(missing_in_master))[:10]}..."
        )

    ok("All optimiser tickers belong to the governed universe")
    ok("Optimiser layer checks passed")


def verify_reporting(
    portfolio_performance: pd.DataFrame,
    factor_exposure: pd.DataFrame,
    turnover: pd.DataFrame,
    optimiser: pd.DataFrame,
) -> None:
    print("\n--- 5. Verifying reporting layer contracts ---\n")

    opt_latest = latest_date(optimiser, "date")

    for name, df, col in [
        ("portfolio_performance_quant_v1", portfolio_performance, "date"),
        ("factor_exposure_report_quant_v1", factor_exposure, "date"),
        ("turnover_regime_quant_v1", turnover, "date"),
    ]:
        if col not in df.columns:
            fail(f"{name}.parquet missing '{col}' column")
        latest_rep = latest_date(df, col)
        if latest_rep != opt_latest:
            fail(
                f"{name}.parquet latest date {latest_rep} does not align with optimiser latest date {opt_latest}"
            )

    ok("Reporting files date-aligned with optimiser")

    for name, df in [
        ("portfolio_performance_quant_v1", portfolio_performance),
        ("factor_exposure_report_quant_v1", factor_exposure),
        ("turnover_regime_quant_v1", turnover),
    ]:
        numeric_cols = df.select_dtypes(include=["number"]).columns
        if numeric_cols.empty:
            continue
        if df[numeric_cols].isna().any().any():
            fail(f"{name}.parquet contains NaNs in numeric metric columns")

    ok("Reporting files have no NaNs in numeric metrics")
    ok("Reporting layer checks passed")


def verify_date_alignment(
    prices: pd.DataFrame,
    exp: pd.DataFrame,
    opt: pd.DataFrame,
) -> None:
    print("\n--- 6. Verifying global date alignment ---\n")

    dates = {
        "prices": latest_date(prices, "date"),
        "expected_returns": latest_date(exp, "date"),
        "optimiser": latest_date(opt, "date"),
    }

    print("Latest dates:")
    for k, v in dates.items():
        print(f"  {k}: {v}")

    if len(set(dates.values())) != 1:
        fail("Date mismatch across pipeline stages")

    ok("All core pipeline stages share the same latest date")


def verify_dashboard_legacy() -> None:
    print("\n--- 7. Verifying dashboard has no legacy references ---\n")

    LEGACY_PATTERNS = [
        r"quant_weekly_10x10",
        r"weekly_selection",
        r"dashboard_10stock_weekly",
    ]

    legacy_hits = []

    if not SCRIPTS_DASHBOARD.exists():
        fail(f"Dashboard scripts directory not found: {SCRIPTS_DASHBOARD}")

    for py in SCRIPTS_DASHBOARD.rglob("*.py"):
        text = py.read_text(errors="ignore")
        for pattern in LEGACY_PATTERNS:
            if re.search(pattern, text):
                legacy_hits.append((py, pattern))
        if "archive" in text:
            legacy_hits.append((py, "archive path reference"))

    if legacy_hits:
        print("\nLegacy references found in dashboard scripts:")
        for path, pattern in legacy_hits:
            print(f"  {path}  →  {pattern}")
        fail("Legacy weekly system or archive paths still referenced in dashboard")

    ok("No legacy weekly or archive references in dashboard scripts")


def main() -> None:
    print("\n=== VERIFYING QUANT V1 BUILD (MODULAR) ===\n")

    verify_file_existence()

    try:
        prices = pd.read_parquet(REQUIRED_FILES["prices"])
        fundamentals = pd.read_parquet(REQUIRED_FILES["fundamentals"])
        risk_model = pd.read_parquet(REQUIRED_FILES["risk_model"])
        securities_master = pd.read_parquet(REQUIRED_FILES["securities_master"])
        exp = pd.read_parquet(REQUIRED_FILES["expected_returns"])
        opt = pd.read_parquet(REQUIRED_FILES["optimiser"])
        portfolio_performance = pd.read_parquet(REQUIRED_FILES["portfolio_performance"])
        factor_exposure = pd.read_parquet(REQUIRED_FILES["factor_exposure_report"])
        turnover = pd.read_parquet(REQUIRED_FILES["turnover_regime"])
    except Exception as e:
        fail(f"Error loading parquet files: {e}")

    verify_ingestion(prices, securities_master)
    verify_signals(exp, prices)
    verify_optimiser(opt, securities_master)
    verify_reporting(portfolio_performance, factor_exposure, turnover, opt)
    verify_date_alignment(prices, exp, opt)
    verify_dashboard_legacy()

    print("\n🎉 VERIFIED: Quant v1 build is aligned, governed, and safe.\n")


if __name__ == "__main__":
    main()