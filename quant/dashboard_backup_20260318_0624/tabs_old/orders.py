import pandas as pd
from dash import html, dcc
import dash_bootstrap_components as dbc

from quant.infrastructure.db import get_engine


def layout():
    """
    Deterministic, DB-backed Orders tab.
    Reads canonical orders table.
    """
    df = load_orders()

    if df.empty:
        return dbc.Alert(
            "No orders available.",
            color="warning",
            className="mt-3",
        )

    return dbc.Container(
        [
            html.H4("Orders"),
            html.Hr(),

            # Chart: order notional over time
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(
                            figure=build_orders_chart(df),
                            id="orders-chart",
                        ),
                        width=12,
                    )
                ]
            ),

            # Table: latest 100 orders
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


def load_orders() -> pd.DataFrame:
    """
    Canonical DB read.
    Assumes a table named 'orders' with columns:
        timestamp (datetime)
        strategy (text)
        symbol (text)
        side (text)
        quantity (float)
        price (float)
        notional (float)
        status (text)
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
            status
        FROM orders
        ORDER BY timestamp ASC
    """

    try:
        df = pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()

    return df


def build_orders_chart(df: pd.DataFrame):
    """
    Deterministic Plotly figure for order notional over time.
    """
    import plotly.express as px

    fig = px.scatter(
        df,
        x="timestamp",
        y="notional",
        color="side",
        size="quantity",
        hover_data=["strategy", "symbol", "status"],
        title="Order Notional Over Time",
    )

    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        legend_title_text="Side",
    )

    return fig