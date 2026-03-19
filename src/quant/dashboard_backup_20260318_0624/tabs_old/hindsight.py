import pandas as pd
from dash import html, dcc, Input, Output, callback
import plotly.graph_objects as go

from quant.infrastructure.db import get_engine


# ------------------------------------------------------------
# DB loaders (strategy, pnl, regimes, events)
# ------------------------------------------------------------
def load_strategies() -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT id, name, COALESCE(label, name) AS label
        FROM strategies
        ORDER BY name;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()


def load_strategy_pnl(strategy_id: int) -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT
            date,
            strategy_id,
            pnl,
            cumulative_pnl,
            return
        FROM strategy_pnl_daily
        WHERE strategy_id = %s
        ORDER BY date;
    """
    try:
        return pd.read_sql(query, engine, params=(strategy_id,))
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


def load_market_events() -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT
            e.id,
            e.event_type,
            e.label,
            l.event_time::date AS date
        FROM market_events e
        JOIN event_log l
          ON l.event_id = e.id
        ORDER BY l.event_time;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()


# ------------------------------------------------------------
# Layout
# ------------------------------------------------------------
def layout():
    strategies = load_strategies()

    return html.Div(
        [
            html.Div(
                [
                    html.Label("Strategy"),
                    dcc.Dropdown(
                        id="hs-strategy-dropdown",
                        options=[
                            {"label": row["label"], "value": row["id"]}
                            for _, row in strategies.iterrows()
                        ],
                        value=strategies["id"].iloc[0] if not strategies.empty else None,
                        clearable=False,
                    ),
                ],
                style={"width": "300px", "marginBottom": "1rem"},
            ),
            dcc.Graph(id="hs-pnl-graph"),
        ]
    )


# ------------------------------------------------------------
# Callback
# ------------------------------------------------------------
@callback(
    Output("hs-pnl-graph", "figure"),
    Input("hs-strategy-dropdown", "value"),
)
def update_hindsight(strategy_id):
    if strategy_id is None:
        return go.Figure()

    pnl = load_strategy_pnl(strategy_id)
    if pnl.empty:
        return go.Figure()

    regime = load_market_regime()
    events = load_market_events()

    df = pnl.merge(regime, on="date", how="left")

    fig = go.Figure()

    # PnL curve
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["cumulative_pnl"],
            mode="lines",
            name="Cumulative PnL",
            line=dict(color="steelblue", width=2),
        )
    )

    # Regime shading
    if "regime" in df.columns:
        unique_regimes = df["regime"].dropna().unique()
        palette = [
            "rgba(200,200,255,0.18)",
            "rgba(200,255,200,0.18)",
            "rgba(255,200,200,0.18)",
            "rgba(240,240,240,0.18)",
        ]
        color_map = {r: palette[i % len(palette)] for i, r in enumerate(sorted(unique_regimes))}

        for r in unique_regimes:
            sub = df[df["regime"] == r]
            if not sub.empty:
                fig.add_vrect(
                    x0=sub["date"].min(),
                    x1=sub["date"].max(),
                    fillcolor=color_map[r],
                    layer="below",
                    line_width=0,
                    opacity=0.18,
                )

    # Event markers
    if not events.empty:
        mask = (events["date"] >= df["date"].min()) & (events["date"] <= df["date"].max())
        ev = events[mask]
        for _, row in ev.iterrows():
            fig.add_vline(
                x=row["date"],
                line=dict(color="orange", width=1, dash="dot"),
                opacity=0.7,
            )

    fig.update_layout(
        title="Strategy Hindsight — Cumulative PnL with Regimes & Events",
        xaxis_title="Date",
        yaxis_title="Cumulative PnL",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=40, r=20, t=60, b=40),
    )

    return fig