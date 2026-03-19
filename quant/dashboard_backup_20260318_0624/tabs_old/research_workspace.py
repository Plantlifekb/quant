import pandas as pd
from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from quant.infrastructure.db import get_engine


# ------------------------------------------------------------
# DB loaders
# ------------------------------------------------------------
def load_signals() -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT DISTINCT signal_name
        FROM signals_daily
        ORDER BY signal_name;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()


def load_signal_series(signal_name: str) -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT
            date,
            ticker,
            value AS signal
        FROM signals_daily
        WHERE signal_name = %s
        ORDER BY date, ticker;
    """
    try:
        return pd.read_sql(query, engine, params=(signal_name,))
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


# ------------------------------------------------------------
# Layout
# ------------------------------------------------------------
def layout():
    signals = load_signals()

    return html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Signal"),
                            dcc.Dropdown(
                                id="rw-signal-dropdown",
                                options=[{"label": s, "value": s} for s in signals["signal_name"]],
                                value=signals["signal_name"].iloc[0] if not signals.empty else None,
                                clearable=False,
                            ),
                        ],
                        width=3,
                    ),
                ],
                className="mb-4",
            ),

            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="rw-signal-timeseries"), width=12),
                ],
                className="mb-4",
            ),

            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="rw-signal-dist"), width=6),
                    dbc.Col(dcc.Graph(id="rw-signal-vs-returns"), width=6),
                ],
                className="mb-4",
            ),

            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="rw-rolling-ic"), width=12),
                ],
                className="mb-4",
            ),

            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="rw-signal-decay"), width=12),
                ]
            ),
        ]
    )


# ------------------------------------------------------------
# Callbacks
# ------------------------------------------------------------
@callback(
    Output("rw-signal-timeseries", "figure"),
    Output("rw-signal-dist", "figure"),
    Output("rw-signal-vs-returns", "figure"),
    Output("rw-rolling-ic", "figure"),
    Output("rw-signal-decay", "figure"),
    Input("rw-signal-dropdown", "value"),
)
def update_research_workspace(signal_name):
    signals = load_signal_series(signal_name)
    returns = load_returns()

    # Merge signals + returns
    df = signals.merge(returns, on=["date", "ticker"], how="inner")

    # ------------------------------------------------------------
    # Signal timeseries (mean across tickers)
    # ------------------------------------------------------------
    fig_ts = go.Figure()

    if not df.empty:
        ts = df.groupby("date")["signal"].mean().reset_index()

        fig_ts.add_trace(
            go.Scatter(
                x=ts["date"],
                y=ts["signal"],
                mode="lines",
                name="Signal",
                line=dict(color="steelblue"),
            )
        )

        fig_ts.update_layout(
            title=f"Signal Timeseries — {signal_name}",
            xaxis_title="Date",
            yaxis_title="Signal",
            hovermode="x unified",
        )

    # ------------------------------------------------------------
    # Signal distribution
    # ------------------------------------------------------------
    fig_dist = go.Figure()

    if not df.empty:
        fig_dist.add_trace(
            go.Histogram(
                x=df["signal"],
                nbinsx=50,
                marker_color="darkorange",
            )
        )

        fig_dist.update_layout(
            title="Signal Distribution",
            xaxis_title="Signal",
            yaxis_title="Frequency",
        )

    # ------------------------------------------------------------
    # Signal vs. returns scatter
    # ------------------------------------------------------------
    fig_scatter = go.Figure()

    if not df.empty:
        fig_scatter.add_trace(
            go.Scatter(
                x=df["signal"],
                y=df["return"],
                mode="markers",
                marker=dict(size=4, color="purple", opacity=0.5),
                name="Signal vs Return",
            )
        )

        fig_scatter.update_layout(
            title="Signal vs. Next-Day Return",
            xaxis_title="Signal",
            yaxis_title="Return",
        )

    # ------------------------------------------------------------
    # Rolling IC (Information Coefficient)
    # ------------------------------------------------------------
    fig_ic = go.Figure()

    if not df.empty:
        # Compute daily cross-sectional IC
        ic = (
            df.groupby("date")
            .apply(lambda x: x["signal"].corr(x["return"]))
            .reset_index(name="ic")
        )

        fig_ic.add_trace(
            go.Scatter(
                x=ic["date"],
                y=ic["ic"],
                mode="lines",
                name="IC",
                line=dict(color="green"),
            )
        )

        fig_ic.update_layout(
            title="Rolling Information Coefficient (IC)",
            xaxis_title="Date",
            yaxis_title="IC",
            hovermode="x unified",
        )

    # ------------------------------------------------------------
    # Signal decay curve
    # ------------------------------------------------------------
    fig_decay = go.Figure()

    if not df.empty:
        # Compute decay: correlation of signal(t) with return(t+lag)
        lags = range(1, 11)
        decay_values = []

        for lag in lags:
            shifted = returns.copy()
            shifted["date"] = shifted["date"] - pd.Timedelta(days=lag)

            merged = signals.merge(shifted, on=["date", "ticker"], how="inner")
            if merged.empty:
                decay_values.append(0)
            else:
                decay_values.append(merged["signal"].corr(merged["return"]))

        fig_decay.add_trace(
            go.Scatter(
                x=list(lags),
                y=decay_values,
                mode="lines+markers",
                name="Decay",
                line=dict(color="red"),
            )
        )

        fig_decay.update_layout(
            title="Signal Decay Curve",
            xaxis_title="Lag (days)",
            yaxis_title="Correlation",
        )

    return fig_ts, fig_dist, fig_scatter, fig_ic, fig_decay