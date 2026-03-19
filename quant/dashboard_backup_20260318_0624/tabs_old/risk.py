import pandas as pd
from dash import html, dcc
import dash_bootstrap_components as dbc

from quant.infrastructure.db import get_engine


def layout():
    """
    Deterministic, DB-backed Risk tab.
    Reads canonical risk table.
    """
    df = load_risk()

    if df.empty:
        return dbc.Alert(
            "No risk data available.",
            color="warning",
            className="mt-3",
        )

    return dbc.Container(
        [
            html.H4("Risk"),
            html.Hr(),

            # Volatility chart
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(
                            figure=build_vol_chart(df),
                            id="risk-vol-chart",
                        ),
                        width=12,
                    )
                ]
            ),

            # VaR chart
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(
                            figure=build_var_chart(df),
                            id="risk-var-chart",
                        ),
                        width=12,
                    )
                ],
                className="mt-4",
            ),

            # Latest snapshot table
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


def load_risk() -> pd.DataFrame:
    """
    Canonical DB read.
    Assumes a table named 'risk' with columns:
        timestamp (datetime)
        strategy (text)
        volatility (float)
        var_95 (float)
        var_99 (float)
    """
    engine = get_engine()

    query = """
        SELECT
            timestamp,
            strategy,
            volatility,
            var_95,
            var_99
        FROM risk
        ORDER BY timestamp ASC
    """

    try:
        df = pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()

    return df


def latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns the most recent timestamp's risk snapshot.
    Deterministic: no guessing.
    """
    if df.empty:
        return df

    latest_ts = df["timestamp"].max()
    return df[df["timestamp"] == latest_ts].sort_values("strategy")


def build_vol_chart(df: pd.DataFrame):
    """
    Deterministic Plotly figure for volatility.
    """
    import plotly.express as px

    fig = px.line(
        df,
        x="timestamp",
        y="volatility",
        color="strategy",
        title="Volatility Over Time",
    )

    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        legend_title_text="Strategy",
    )

    return fig


def build_var_chart(df: pd.DataFrame):
    """
    Deterministic Plotly figure for VaR.
    """
    import plotly.express as px

    fig = px.line(
        df,
        x="timestamp",
        y="var_95",
        color="strategy",
        title="95% VaR Over Time",
    )

    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        legend_title_text="Strategy",
    )

    return fig