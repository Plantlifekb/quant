import pandas as pd
from dash import html, dcc
import dash_bootstrap_components as dbc

from quant.infrastructure.db import get_engine


def layout():
    """
    Deterministic, DB-backed Portfolio tab.
    Reads canonical portfolio table.
    """
    df = load_portfolio()

    if df.empty:
        return dbc.Alert(
            "No portfolio data available.",
            color="warning",
            className="mt-3",
        )

    return dbc.Container(
        [
            html.H4("Portfolio"),
            html.Hr(),

            # Chart: portfolio total value over time
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(
                            figure=build_portfolio_value_chart(df),
                            id="portfolio-value-chart",
                        ),
                        width=12,
                    )
                ]
            ),

            # Chart: allocation by symbol at latest timestamp
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(
                            figure=build_allocation_pie(df),
                            id="portfolio-allocation-chart",
                        ),
                        width=6,
                    ),
                    dbc.Col(
                        dbc.Table.from_dataframe(
                            latest_snapshot(df),
                            striped=True,
                            bordered=True,
                            hover=True,
                            size="sm",
                        ),
                        width=6,
                    ),
                ],
                className="mt-4",
            ),
        ],
        fluid=True,
        className="mt-3",
    )


def load_portfolio() -> pd.DataFrame:
    """
    Canonical DB read.
    Assumes a table named 'portfolio' with columns:
        timestamp (datetime)
        symbol (text)
        quantity (float)
        price (float)
        notional (float)
        total_value (float)
    """
    engine = get_engine()

    query = """
        SELECT
            timestamp,
            symbol,
            quantity,
            price,
            notional,
            total_value
        FROM portfolio
        ORDER BY timestamp ASC
    """

    try:
        df = pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()

    return df


def latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns the most recent timestamp's portfolio snapshot.
    Deterministic: no guessing.
    """
    if df.empty:
        return df

    latest_ts = df["timestamp"].max()
    return df[df["timestamp"] == latest_ts].sort_values("symbol")


def build_portfolio_value_chart(df: pd.DataFrame):
    """
    Deterministic Plotly figure for total portfolio value.
    """
    import plotly.express as px

    fig = px.line(
        df,
        x="timestamp",
        y="total_value",
        title="Total Portfolio Value Over Time",
    )

    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
    )

    return fig


def build_allocation_pie(df: pd.DataFrame):
    """
    Pie chart of symbol allocation at latest timestamp.
    """
    import plotly.express as px

    snap = latest_snapshot(df)
    if snap.empty:
        return {}

    fig = px.pie(
        snap,
        names="symbol",
        values="notional",
        title="Portfolio Allocation (Latest Snapshot)",
    )

    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
    )

    return fig