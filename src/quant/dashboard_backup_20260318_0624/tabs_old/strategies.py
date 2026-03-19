import pandas as pd
from datetime import datetime, timedelta

from dash import html, dcc
import dash_bootstrap_components as dbc

from quant.infrastructure.db import get_engine


def layout():
    """
    Deterministic, DB-backed Strategies tab.

    Shows:
        - Strategy performance over the past 5 years
        - Event overlays (COVID, wars, etc.) if available
        - Tabular view of cumulative performance by strategy
    """
    perf = load_strategy_performance()
    events = load_market_events()

    if perf.empty:
        return dbc.Alert(
            "No strategy performance data available.",
            color="warning",
            className="mt-3",
        )

    fig = build_strategy_performance_figure(perf, events)

    summary = build_strategy_summary(perf)

    return dbc.Container(
        [
            html.H4("Strategies – 5-Year Performance"),
            html.Hr(),

            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(
                            figure=fig,
                            id="strategy-performance-chart",
                        ),
                        width=12,
                    )
                ]
            ),

            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H5("Strategy Performance Summary (5 Years)"),
                            summary,
                        ],
                        width=12,
                        className="mt-4",
                    )
                ]
            ),
        ],
        fluid=True,
        className="mt-3",
    )


def load_strategy_performance() -> pd.DataFrame:
    """
    Canonical DB read for strategy performance.

    Assumes table 'strategy_performance' with:
        date      (date/datetime)
        strategy  (text)
        return    (float, periodic return)

    Filters to last 5 years.
    """
    engine = get_engine()

    query = """
        SELECT
            date,
            strategy,
            return
        FROM strategy_performance
        WHERE date >= CURRENT_DATE - INTERVAL '5 years'
        ORDER BY date ASC
    """

    try:
        df = pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return df

    # Ensure datetime and sorted
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["strategy", "date"])

    # Compute cumulative return per strategy
    # Assuming 'return' is simple periodic return (e.g. daily)
    df["cum_return"] = (
        1.0 + df["return"]
    ).groupby(df["strategy"]).cumprod() - 1.0

    return df


def load_market_events() -> pd.DataFrame:
    """
    Canonical DB read for market events.

    Assumes table 'market_events' with:
        name       (text)
        start_date (date/datetime)
        end_date   (date/datetime)

    Used to overlay regimes (COVID, wars, etc.) on strategy performance.
    """
    engine = get_engine()

    query = """
        SELECT
            name,
            start_date,
            end_date
        FROM market_events
        ORDER BY start_date ASC
    """

    try:
        df = pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return df

    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])

    return df


def build_strategy_performance_figure(perf: pd.DataFrame, events: pd.DataFrame):
    """
    Build a Plotly figure of cumulative strategy performance over time,
    with shaded regions for major market events if available.
    """
    import plotly.express as px

    fig = px.line(
        perf,
        x="date",
        y="cum_return",
        color="strategy",
        title="Strategy Cumulative Performance (Last 5 Years)",
        labels={"cum_return": "Cumulative Return", "date": "Date"},
    )

    # Add event overlays as shaded regions
    if not events.empty:
        for _, row in events.iterrows():
            fig.add_vrect(
                x0=row["start_date"],
                x1=row["end_date"],
                fillcolor="grey",
                opacity=0.2,
                layer="below",
                line_width=0,
                annotation_text=row["name"],
                annotation_position="top left",
            )

    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        legend_title_text="Strategy",
    )

    return fig


def build_strategy_summary(perf: pd.DataFrame):
    """
    Build a small summary table of strategy performance over the last 5 years.
    """
    # Aggregate by strategy
    summary = (
        perf.groupby("strategy")
        .agg(
            start_date=("date", "min"),
            end_date=("date", "max"),
            total_return=("cum_return", "last"),
            avg_period_return=("return", "mean"),
            periods=("return", "count"),
        )
        .reset_index()
    )

    # Format for readability
    summary["total_return"] = (summary["total_return"] * 100.0).round(2)
    summary["avg_period_return"] = (summary["avg_period_return"] * 100.0).round(3)

    summary = summary.rename(
        columns={
            "strategy": "Strategy",
            "start_date": "Start",
            "end_date": "End",
            "total_return": "Total Return (%)",
            "avg_period_return": "Avg Period Return (%)",
            "periods": "Periods",
        }
    )

    table = dbc.Table.from_dataframe(
        summary,
        striped=True,
        bordered=True,
        hover=True,
        size="sm",
    )

    return table