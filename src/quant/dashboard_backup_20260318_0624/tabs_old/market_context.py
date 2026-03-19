import pandas as pd
from dash import html, dcc, Input, Output, callback
import plotly.graph_objects as go
import dash_bootstrap_components as dbc

from quant.infrastructure.db import get_engine


# ------------------------------------------------------------
# DB loaders
# ------------------------------------------------------------
def load_breadth() -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT date, advancers, decliners, adv_dec_ratio
        FROM breadth_daily
        ORDER BY date;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()


def load_vol_regime() -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT date, vol_regime
        FROM volatility_regime_daily
        ORDER BY date;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()


def load_market_regime() -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT date, regime
        FROM market_regime_daily
        ORDER BY date;
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
            volume,
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
                    dbc.Col(dcc.Graph(id="mc-breadth-graph"), width=12),
                ],
                className="mb-4",
            ),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="mc-vol-regime-graph"), width=12),
                ],
                className="mb-4",
            ),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="mc-market-regime-graph"), width=12),
                ],
                className="mb-4",
            ),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="mc-metrics-graph"), width=12),
                ],
                className="mb-4",
            ),
        ]
    )


# ------------------------------------------------------------
# Callbacks
# ------------------------------------------------------------
@callback(
    Output("mc-breadth-graph", "figure"),
    Output("mc-vol-regime-graph", "figure"),
    Output("mc-market-regime-graph", "figure"),
    Output("mc-metrics-graph", "figure"),
    Input("main-tabs", "value"),
)
def update_market_context(_active_tab):
    breadth = load_breadth()
    vol_regime = load_vol_regime()
    market_regime = load_market_regime()
    metrics = load_market_metrics()

    # ------------------------------------------------------------
    # Breadth graph
    # ------------------------------------------------------------
    fig_breadth = go.Figure()
    if not breadth.empty:
        fig_breadth.add_trace(
            go.Scatter(
                x=breadth["date"],
                y=breadth["adv_dec_ratio"],
                mode="lines",
                name="Adv/Dec Ratio",
                line=dict(color="steelblue"),
            )
        )
        fig_breadth.update_layout(
            title="Market Breadth (Adv/Dec Ratio)",
            xaxis_title="Date",
            yaxis_title="Adv/Dec Ratio",
            hovermode="x unified",
        )

    # ------------------------------------------------------------
    # Volatility regime graph
    # ------------------------------------------------------------
    fig_vol = go.Figure()
    if not vol_regime.empty:
        fig_vol.add_trace(
            go.Scatter(
                x=vol_regime["date"],
                y=vol_regime["vol_regime"],
                mode="lines",
                name="Volatility Regime",
                line=dict(color="darkorange"),
            )
        )
        fig_vol.update_layout(
            title="Volatility Regime",
            xaxis_title="Date",
            yaxis_title="Regime",
            hovermode="x unified",
        )

    # ------------------------------------------------------------
    # Market regime graph
    # ------------------------------------------------------------
    fig_regime = go.Figure()
    if not market_regime.empty:
        fig_regime.add_trace(
            go.Scatter(
                x=market_regime["date"],
                y=market_regime["regime"],
                mode="lines",
                name="Market Regime",
                line=dict(color="purple"),
            )
        )
        fig_regime.update_layout(
            title="Market Regime",
            xaxis_title="Date",
            yaxis_title="Regime",
            hovermode="x unified",
        )

    # ------------------------------------------------------------
    # Market metrics graph
    # ------------------------------------------------------------
    fig_metrics = go.Figure()
    if not metrics.empty:
        fig_metrics.add_trace(
            go.Scatter(
                x=metrics["date"],
                y=metrics["volume"],
                mode="lines",
                name="Volume",
                line=dict(color="steelblue"),
                yaxis="y1",
            )
        )
        fig_metrics.add_trace(
            go.Scatter(
                x=metrics["date"],
                y=metrics["volatility"],
                mode="lines",
                name="Volatility",
                line=dict(color="darkorange"),
                yaxis="y2",
            )
        )
        fig_metrics.add_trace(
            go.Scatter(
                x=metrics["date"],
                y=metrics["liquidity"],
                mode="lines",
                name="Liquidity",
                line=dict(color="green"),
                yaxis="y3",
            )
        )
        fig_metrics.add_trace(
            go.Scatter(
                x=metrics["date"],
                y=metrics["spread"],
                mode="lines",
                name="Spread",
                line=dict(color="red"),
                yaxis="y4",
            )
        )

        fig_metrics.update_layout(
            title="Market Metrics (Volume, Volatility, Liquidity, Spread)",
            hovermode="x unified",
            xaxis=dict(domain=[0.05, 0.95]),
            yaxis=dict(title="Volume", side="left"),
            yaxis2=dict(title="Volatility", overlaying="y", side="right"),
            yaxis3=dict(title="Liquidity", overlaying="y", side="left", position=0.02),
            yaxis4=dict(title="Spread", overlaying="y", side="right", position=0.98),
        )

    return fig_breadth, fig_vol, fig_regime, fig_metrics