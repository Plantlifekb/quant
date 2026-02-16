import pandas as pd


def build_return_summary(prices_enriched: pd.DataFrame) -> pd.DataFrame:
    """
    Build high-level return summary per ticker for dashboard tiles.

    Inputs
    -------
    prices_enriched : DataFrame from analytics_suite_runner["prices_enriched"]

    Outputs
    -------
    summary DataFrame with one row per ticker:
        - ticker
        - cum_return_last
        - weekly_return_last
        - monthly_return_last
        - annual_return_last
        - vol_20d_last
    """

    # take last observation per ticker
    last = (
        prices_enriched
        .sort_values(["ticker", "date"])
        .groupby("ticker")
        .tail(1)
        .copy()
    )

    summary = last[[
        "ticker",
        "cum_return",
        "weekly_return",
        "monthly_return",
        "annual_return",
        "vol_20d",
    ]].rename(columns={
        "cum_return": "cum_return_last",
        "weekly_return": "weekly_return_last",
        "monthly_return": "monthly_return_last",
        "annual_return": "annual_return_last",
        "vol_20d": "vol_20d_last",
    })

    return summary


def build_reporting_bundle(prices_enriched: pd.DataFrame) -> dict:
    """
    Bundle reporting artifacts for the dashboard.

    Outputs
    -------
    dict with:
        - return_summary : per-ticker summary for tiles
        # - more panels can be added later
    """

    return_summary = build_return_summary(prices_enriched)

    return {
        "return_summary": return_summary,
    }