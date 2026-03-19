# quant/dashboard/tabs/strategy_hindsight.py
import pandas as pd
from dash import html, dcc, Input, Output, callback
import plotly.graph_objects as go

from quant.dashboard.data_access import (
    load_strategies,
    load_strategy_pnl_daily,
    load_market_regime,
    load_market_events,
)

def layout():
    strategies_df = load_strategies()

    return html.Div(
        [
            html.Div(
                [
                    html.Label("Strategy"),
                    dcc.Dropdown(
                        id="sh-strategy-dropdown",
                        options=[
                            {"label": row["label"], "value": row["id"]}
                            for _, row in strategies_df.iterrows()
                        ],
                        value=strategies_df["id"].iloc[0] if not strategies_df.empty else None,
                        clearable=False,
                    ),
                ],
                style={"width": "300px", "marginBottom": "1rem"},
            ),
            dcc.Graph(id="sh-pnl-graph"),
        ]
    )

@callback(
    Output("sh-pnl-graph", "figure"),
    Input("sh-strategy-dropdown", "value"),
)
def update_strategy_hindsight(strategy_id):
    if strategy_id is None:
        return go.Figure()

    pnl = load_strategy_pnl_daily(strategy_id)
    if pnl.empty:
        return go.Figure()

    regime = load_market_regime()
    events = load_market_events()

    df = pnl.merge(regime, on="date", how="left")

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["cumulative_pnl"],
            mode="lines",
            name="Cumulative PnL",
            line=dict(color="steelblue"),
        )
    )

    # Regime as background bands (optional simple version)
    if "regime" in df.columns:
        unique_regimes = df["regime"].dropna().unique()
        colors = {
            r: c for r, c in zip(
                sorted(unique_regimes),
                ["rgba(200,200,255,0.2)", "rgba(200,255,200,0.2)", "rgba(255,200,200,0.2)", "rgba(240,240,240,0.2)"]
            )
        }
        for regime_value in unique_regimes:
            sub = df[df["regime"] == regime_value]
            if sub.empty:
                continue
            fig.add_vrect(
                x0=sub["date"].min(),
                x1=sub["date"].max(),
                fillcolor=colors.get(regime_value, "rgba(220,220,220,0.15)"),
                layer="below",
                line_width=0,
                annotation_text=str(regime_value),
                annotation_position="top left",
                opacity=0.15,
            )

    # Event markers
    if not events.empty:
        # align to strategy date range
        mask = (events["date"] >= df["date"].min()) & (events["date"] <= df["date"].max())
        ev = events[mask]
        for _, row in ev.iterrows():
            fig.add_vline(
                x=row["date"],
                line=dict(color="orange", width=1, dash="dot"),
                opacity=0.7,
            )

    fig.update_layout(
        title="Strategy Hindsight – Cumulative PnL with Regimes & Events",
        xaxis_title="Date",
        yaxis_title="Cumulative PnL",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )

    return fig