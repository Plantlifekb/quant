import pandas as pd
from dash import html, dcc
import dash_bootstrap_components as dbc

from quant.infrastructure.db import get_engine


def layout():
    """
    Deterministic, DB-backed Returns tab.
    No hidden state. No drift. Pure read from the canonical returns table.
    """
    df = load_returns()

    if df.empty:
        return dbc.Alert(
            "No returns data available.",
            color="warning",
            className="mt-3",
        )

    return dbc.Container(
        [
            html.H4("Returns"),
            html.Hr(),

            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(
                            figure=build_returns_chart(df),
                            id="returns-chart",
                        ),
                        width=12,
                    )
                ]
            ),

            dbc.Row(
                [
                    dbc.Col(
                        dbc.Table.from_dataframe(
                            df.tail(50),
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


def load_returns() -> pd.DataFrame:
    """
    Canonical DB read.
    Assumes a table named 'returns' with columns:
        timestamp (datetime)
        strategy (text)
        return (float)
    """
    engine = get_engine()

    query = """
        SELECT
            timestamp,
            strategy,
            return
        FROM returns
        ORDER BY timestamp ASC
    """

    try:
        df = pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()

    return df


def build_returns_chart(df: pd.DataFrame):
    """
    Deterministic Plotly figure for returns.
    """
    import plotly.express as px

    fig = px.line(
        df,
        x="timestamp",
        y="return",
        color="strategy",
        title="Strategy Returns Over Time",
    )

    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        legend_title_text="Strategy",
    )

    return fig