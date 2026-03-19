import pandas as pd
from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from quant.infrastructure.db import get_engine


# ------------------------------------------------------------
# DB loaders
# ------------------------------------------------------------
def load_positions() -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT
            date,
            strategy_id,
            ticker,
            position,
            exposure
        FROM strategy_positions_daily
        ORDER BY date, strategy_id, ticker;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()


def load_pnl() -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT
            date,
            strategy_id,
            cumulative_pnl,
            pnl
        FROM strategy_pnl_daily
        ORDER BY date, strategy_id;
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


def load_top_tickers() -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT
            date,
            ticker,
            weight
        FROM top_tickers
        ORDER BY date, weight DESC;
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
                    dbc.Col(dcc.Graph(id="pc-pnl-graph"), width=12),
                ],
                className="mb-4",
            ),

            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="pc-exposure-graph"), width=12),
                ],
                className="mb-4",
            ),

            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="pc-concentration-graph"), width=12),
                ],
                className="mb-4",
            ),

            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="pc-top-tickers-graph"), width=12),
                ]
            ),
        ]
    )


# ------------------------------------------------------------
# Callbacks
# ------------------------------------------------------------
@callback(
    Output("pc-pnl-graph", "figure"),
    Output("pc-exposure-graph", "figure"),
    Output("pc-concentration-graph", "figure"),
    Output("pc-top-tickers-graph", "figure"),
    Input("main-tabs", "value"),
)
def update_portfolio_console(_active_tab):
    positions = load_positions()
    pnl = load_pnl()
    top = load_top_tickers()

    # ------------------------------------------------------------
    # Portfolio PnL (sum across strategies)
    # ------------------------------------------------------------
    fig_pnl = go.Figure()

    if not pnl.empty:
        agg = pnl.groupby("date")["cumulative_pnl"].sum().reset_index()

        fig_pnl.add_trace(
            go.Scatter(
                x=agg["date"],
                y=agg["cumulative_pnl"],
                mode="lines",
                name="Portfolio Cumulative PnL",
                line=dict(color="steelblue"),
            )
        )

        fig_pnl.update_layout(
            title="Portfolio Cumulative PnL",
            xaxis_title="Date",
            yaxis_title="Cumulative PnL",
            hovermode="x unified",
        )

    # ------------------------------------------------------------
    # Portfolio Exposure (sum across strategies)
    # ------------------------------------------------------------
    fig_exp = go.Figure()

    if not positions.empty:
        agg = positions.groupby("date")["exposure"].sum().reset_index()

        fig_exp.add_trace(
            go.Scatter(
                x=agg["date"],
                y=agg["exposure"],
                mode="lines",
                name="Total Exposure",
                line=dict(color="darkorange"),
            )
        )

        fig_exp.update_layout(
            title="Portfolio Exposure Over Time",
            xaxis_title="Date",
            yaxis_title="Exposure",
            hovermode="x unified",
        )

    # ------------------------------------------------------------
    # Concentration (Herfindahl index)
    # ------------------------------------------------------------
    fig_conc = go.Figure()

    if not positions.empty:
        pivot = positions.pivot_table(
            index="date",
            columns="ticker",
            values="exposure",
            aggfunc="sum",
            fill_value=0,
        )

        weights = pivot.div(pivot.sum(axis=1), axis=0)
        herfindahl = (weights ** 2).sum(axis=1)

        fig_conc.add_trace(
            go.Scatter(
                x=herfindahl.index,
                y=herfindahl.values,
                mode="lines",
                name="Herfindahl Index",
                line=dict(color="purple"),
            )
        )

        fig_conc.update_layout(
            title="Portfolio Concentration (Herfindahl Index)",
            xaxis_title="Date",
            yaxis_title="Concentration",
            hovermode="x unified",
        )

    # ------------------------------------------------------------
    # Top Tickers (weights)
    # ------------------------------------------------------------
    fig_top = go.Figure()

    if not top.empty:
        latest = top[top["date"] == top["date"].max()].sort_values("weight", ascending=False).head(10)

        fig_top.add_trace(
            go.Bar(
                x=latest["ticker"],
                y=latest["weight"],
                marker_color="steelblue",
            )
        )

        fig_top.update_layout(
            title="Top Tickers (Latest Date)",
            xaxis_title="Ticker",
            yaxis_title="Weight",
        )

    return fig_pnl, fig_exp, fig_conc, fig_top