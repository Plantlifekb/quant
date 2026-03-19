import pandas as pd
from dash import html, dcc
import dash_bootstrap_components as dbc

from quant.infrastructure.db import get_engine


def layout():
    """
    Deterministic, DB-backed Signals tab.
    Reads canonical signals table.
    """
    df = load_signals()

    if df.empty:
        return dbc.Alert(
            "No signals available.",
            color="warning",
            className="mt-3",
        )

    return dbc.Container(
        [
            html.H4("Signals"),
            html.Hr(),

            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(
                            figure=build_signal_chart(df),
                            id="signals-chart",
                        ),
                        width=12,
                    )
                ]
            ),

            dbc.Row(
                [
                    dbc.Col(
                        dbc.Table.from_dataframe(
                            df.tail(100),
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


def load_signals() -> pd.DataFrame:
    """
    Canonical DB read.
    Assumes a table named 'signals' with columns:
        timestamp (datetime)
        strategy (text)
        signal (float)
    """
    engine = get_engine()

    query = """
        SELECT
            timestamp,
            strategy,
            signal
        FROM signals
        ORDER BY timestamp ASC
    """

    try:
        df = pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()

    return df


def build_signal_chart(df: pd.DataFrame):
    """
    Deterministic Plotly figure for signals.
    """
    import plotly.express as px

    fig = px.line(
        df,
        x="timestamp",
        y="signal",
        color="strategy",
        title="Strategy Signals Over Time",
    )

    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        legend_title_text="Strategy",
    )

    return fig