import pandas as pd


def build_return_summary(prices_enriched: pd.DataFrame) -> pd.DataFrame:
    """
    Build high-level return summary per ticker for dashboard tiles.
    """

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


def write_dashboard_inputs_v2(prices_enriched: pd.DataFrame, output_path: str) -> None:
    """
    Write the v2 dashboard input file.
    """
    summary = build_return_summary(prices_enriched)
    summary.to_csv(output_path, index=False)