import pandas as pd
from dash import html, dcc
import dash_bootstrap_components as dbc

from quant.infrastructure.db import get_engine


def layout():
    """
    Deterministic, DB-backed Positions tab.
    Reads canonical positions table.
    """
    df = load_positions()

    if df.empty:
        return dbc.Alert(
            "No positions available.",
            color="warning",
            className="mt-3",
        )

    return dbc.Container(
        [
            html.H4("Positions"),
            html.Hr(),

            # Chart: position sizes over time
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(
                            figure=build_positions_chart(df),
                            id="positions-chart",
                        ),
                        width=12,
                    )
                ]
            ),

            # Table: latest snapshot
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Table.from_dataframe(
                            latest_snapshot(df),
                            striped=True,
                            bordered=True,
                            hover=True,
                            size="sm",
                        ),
                        width=12,
                        className="mt-4",
                    )
                ]
            ),
        ],
        fluid=True,
        className="mt-3",
    )


def load_positions() -> pd.DataFrame:
    """
    Canonical DB read.
    Assumes a table named 'positions' with columns:
        timestamp (datetime)
        strategy (text)
        symbol (text)
        quantity (float)
        notional (float)
    """
    engine = get_engine()

    query = """
        SELECT
            timestamp,
            strategy,
            symbol,
            quantity,
            notional
        FROM positions
        ORDER BY timestamp ASC
    """

    try:
        df = pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()

    return df


def latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns the most recent timestamp's positions.
    Deterministic: no guessing.
    """
    if df.empty:
        return df

    latest_ts = df["timestamp"].max()
    return df[df["timestamp"] == latest_ts].sort_values(["strategy", "symbol"])


def build_positions_chart(df: pd.DataFrame):
    """
    Deterministic Plotly figure for position quantities.
    """
    import plotly.express as px

    fig = px.line(
        df,
        x="timestamp",
        y="quantity",
        color="symbol",
        title="Position Quantities Over Time",
    )

    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        legend_title_text="Symbol",
    )

    return fig