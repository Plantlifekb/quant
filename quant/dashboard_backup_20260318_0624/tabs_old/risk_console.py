import pandas as pd
from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from quant.infrastructure.db import get_engine


# ------------------------------------------------------------
# DB loaders
# ------------------------------------------------------------
def load_strategy_risk() -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT
            strategy_id,
            date,
            beta,
            volatility,
            var,
            cvar,
            exposure
        FROM strategy_risk
        ORDER BY date, strategy_id;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()


def load_positions() -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT
            date,
            strategy_id,
            ticker,
            exposure
        FROM strategy_positions_daily
        ORDER BY date, strategy_id, ticker;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()


def load_returns() -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT
            date,
            ticker,
            return
        FROM returns_daily
        ORDER BY date, ticker;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()


def load_market_metrics() -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT
            date,
            volatility,
            liquidity,
            spread
        FROM market_metrics_daily
        ORDER BY date;
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
                    dbc.Col(dcc.Graph(id="rc-beta-graph"), width=12),
                ],
                className="mb-4",
            ),

            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="rc-vol-graph"), width=12),
                ],
                className="mb-4",
            ),

            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="rc-var-graph"), width=12),
                ],
                className="mb-4",
            ),

            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="rc-exposure-graph"), width=12),
                ]
            ),
        ]
    )


# ------------------------------------------------------------
# Callbacks
# ------------------------------------------------------------
@callback(
    Output("rc-beta-graph", "figure"),
    Output("rc-vol-graph", "figure"),
    Output("rc-var-graph", "figure"),
    Output("rc-exposure-graph", "figure"),
    Input("main-tabs", "value"),
)
def update_risk_console(_active_tab):
    risk = load_strategy_risk()

    # ------------------------------------------------------------
    # Beta graph
    # ------------------------------------------------------------
    fig_beta = go.Figure()

    if not risk.empty:
        agg = risk.groupby("date")["beta"].mean().reset_index()

        fig_beta.add_trace(
            go.Scatter(
                x=agg["date"],
                y=agg["beta"],
                mode="lines",
                name="Portfolio Beta",
                line=dict(color="steelblue"),
            )
        )

        fig_beta.update_layout(
            title="Portfolio Beta Over Time",
            xaxis_title="Date",
            yaxis_title="Beta",
            hovermode="x unified",
        )

    # ------------------------------------------------------------
    # Volatility graph
    # ------------------------------------------------------------
    fig_vol = go.Figure()

    if not risk.empty:
        agg = risk.groupby("date")["volatility"].mean().reset_index()

        fig_vol.add_trace(
            go.Scatter(
                x=agg["date"],
                y=agg["volatility"],
                mode="lines",
                name="Portfolio Volatility",
                line=dict(color="darkorange"),
            )
        )

        fig_vol.update_layout(
            title="Portfolio Volatility Over Time",
            xaxis_title="Date",
            yaxis_title="Volatility",
            hovermode="x unified",
        )

    # ------------------------------------------------------------
    # VaR / CVaR graph
    # ------------------------------------------------------------
    fig_var = go.Figure()

    if not risk.empty:
        agg = risk.groupby("date")[["var", "cvar"]].mean().reset_index()

        fig_var.add_trace(
            go.Scatter(
                x=agg["date"],
                y=agg["var"],
                mode="lines",
                name="VaR",
                line=dict(color="purple"),
            )
        )

        fig_var.add_trace(
            go.Scatter(
                x=agg["date"],
                y=agg["cvar"],
                mode="lines",
                name="CVaR",
                line=dict(color="red"),
            )
        )

        fig_var.update_layout(
            title="Portfolio VaR / CVaR Over Time",
            xaxis_title="Date",
            yaxis_title="Risk",
            hovermode="x unified",
        )

    # ------------------------------------------------------------
    # Exposure graph
    # ------------------------------------------------------------
    fig_exp = go.Figure()

    if not risk.empty:
        agg = risk.groupby("date")["exposure"].sum().reset_index()

        fig_exp.add_trace(
            go.Scatter(
                x=agg["date"],
                y=agg["exposure"],
                mode="lines",
                name="Total Exposure",
                line=dict(color="green"),
            )
        )

        fig_exp.update_layout(
            title="Portfolio Exposure Over Time",
            xaxis_title="Date",
            yaxis_title="Exposure",
            hovermode="x unified",
        )

    return fig_beta, fig_vol, fig_var, fig_exp