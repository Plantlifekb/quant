import pandas as pd
from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from quant.infrastructure.db import get_engine


# ------------------------------------------------------------
# DB loaders
# ------------------------------------------------------------
def load_orders() -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT
            id,
            strategy_id,
            timestamp,
            ticker,
            side,
            quantity,
            price AS order_price
        FROM orders
        ORDER BY timestamp;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()


def load_executions() -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT
            id,
            order_id,
            timestamp,
            ticker,
            quantity,
            price AS exec_price
        FROM executions
        ORDER BY timestamp;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()


def load_strategy_pnl() -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT
            date,
            strategy_id,
            pnl,
            cumulative_pnl
        FROM strategy_pnl_daily
        ORDER BY date, strategy_id;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()


# ------------------------------------------------------------
# Layout
# ------------------------------------------------------------
def layout():
    return html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="ec-order-flow-graph"), width=12),
                ],
                className="mb-4",
            ),

            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="ec-exec-flow-graph"), width=12),
                ],
                className="mb-4",
            ),

            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="ec-fill-rate-graph"), width=12),
                ],
                className="mb-4",
            ),

            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="ec-slippage-graph"), width=12),
                ]
            ),
        ]
    )


# ------------------------------------------------------------
# Callbacks
# ------------------------------------------------------------
@callback(
    Output("ec-order-flow-graph", "figure"),
    Output("ec-exec-flow-graph", "figure"),
    Output("ec-fill-rate-graph", "figure"),
    Output("ec-slippage-graph", "figure"),
    Input("main-tabs", "value"),
)
def update_executions_console(_active_tab):
    orders = load_orders()
    execs = load_executions()

    # ------------------------------------------------------------
    # Order flow graph
    # ------------------------------------------------------------
    fig_orders = go.Figure()

    if not orders.empty:
        daily_orders = orders.groupby(orders["timestamp"].dt.date)["quantity"].sum().reset_index()

        fig_orders.add_trace(
            go.Bar(
                x=daily_orders["timestamp"],
                y=daily_orders["quantity"],
                name="Order Quantity",
                marker_color="steelblue",
            )
        )

        fig_orders.update_layout(
            title="Order Flow (Quantity per Day)",
            xaxis_title="Date",
            yaxis_title="Quantity",
        )

    # ------------------------------------------------------------
    # Execution flow graph
    # ------------------------------------------------------------
    fig_execs = go.Figure()

    if not execs.empty:
        daily_execs = execs.groupby(execs["timestamp"].dt.date)["quantity"].sum().reset_index()

        fig_execs.add_trace(
            go.Bar(
                x=daily_execs["timestamp"],
                y=daily_execs["quantity"],
                name="Executed Quantity",
                marker_color="darkorange",
            )
        )

        fig_execs.update_layout(
            title="Execution Flow (Quantity per Day)",
            xaxis_title="Date",
            yaxis_title="Quantity",
        )

    # ------------------------------------------------------------
    # Fill rate graph
    # ------------------------------------------------------------
    fig_fill = go.Figure()

    if not orders.empty and not execs.empty:
        merged = execs.merge(orders, left_on="order_id", right_on="id", suffixes=("_exec", "_order"))

        daily = merged.groupby(merged["timestamp_exec"].dt.date).agg(
            ordered=("quantity_order", "sum"),
            executed=("quantity_exec", "sum"),
        )

        daily["fill_rate"] = daily["executed"] / daily["ordered"]

        fig_fill.add_trace(
            go.Scatter(
                x=daily.index,
                y=daily["fill_rate"],
                mode="lines",
                name="Fill Rate",
                line=dict(color="green"),
            )
        )

        fig_fill.update_layout(
            title="Fill Rate Over Time",
            xaxis_title="Date",
            yaxis_title="Fill Rate",
            hovermode="x unified",
        )

    # ------------------------------------------------------------
    # Slippage graph
    # ------------------------------------------------------------
    fig_slip = go.Figure()

    if not orders.empty and not execs.empty:
        merged = execs.merge(orders, left_on="order_id", right_on="id", suffixes=("_exec", "_order"))

        merged["slippage"] = merged["exec_price"] - merged["order_price"]

        daily_slip = merged.groupby(merged["timestamp_exec"].dt.date)["slippage"].mean()

        fig_slip.add_trace(
            go.Scatter(
                x=daily_slip.index,
                y=daily_slip.values,
                mode="lines",
                name="Slippage",
                line=dict(color="red"),
            )
        )

        fig_slip.update_layout(
            title="Average Slippage Over Time",
            xaxis_title="Date",
            yaxis_title="Slippage",
            hovermode="x unified",
        )

    return fig_orders, fig_execs, fig_fill, fig_slip