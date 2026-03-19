import pandas as pd
from dash import html, dcc
import dash_bootstrap_components as dbc

from quant.infrastructure.db import get_engine


def layout():
    """
    Deterministic, DB-backed Executions tab.
    Reads canonical executions table.
    """
    df = load_executions()

    if df.empty:
        return dbc.Alert(
            "No executions available.",
            color="warning",
            className="mt-3",
        )

    return dbc.Container(
        [
            html.H4("Executions"),
            html.Hr(),

            # Chart: execution notional over time
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(
                            figure=build_executions_chart(df),
                            id="executions-chart",
                        ),
                        width=12,
                    )
                ]
            ),

            # Table: latest 100 executions
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


def load_executions() -> pd.DataFrame:
    """
    Canonical DB read.
    Assumes a table named 'executions' with columns:
        timestamp (datetime)
        strategy (text)
        symbol (text)
        side (text)
        quantity (float)
        price (float)
        notional (float)
        order_id (text)
        execution_id (text)
    """
    engine = get_engine()

    query = """
        SELECT
            timestamp,
            strategy,
            symbol,
            side,
            quantity,
            price,
            notional,
            order_id,
            execution_id
        FROM executions
        ORDER BY timestamp ASC
    """

    try:
        df = pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()

    return df


def build_executions_chart(df: pd.DataFrame):
    """
    Deterministic Plotly figure for execution notional.
    """
    import plotly.express as px

    fig = px.scatter(
        df,
        x="timestamp",
        y="notional",
        color="side",
        size="quantity",
        hover_data=["strategy", "symbol", "order_id", "execution_id"],
        title="Execution Notional Over Time",
    )

    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        legend_title_text="Side",
    )

    return fig