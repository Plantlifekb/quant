import pandas as pd
from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from quant.infrastructure.db import get_engine


# ------------------------------------------------------------
# DB loaders
# ------------------------------------------------------------
def load_events() -> pd.DataFrame:
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
        SELECT date, cumulative_pnl, return
        FROM strategy_pnl_daily
        WHERE strategy_id = %s
        ORDER BY date;
    """
    try:
        return pd.read_sql(query, engine, params=(strategy_id,))
    except Exception:
        return pd.DataFrame()


# ------------------------------------------------------------
# Layout
# ------------------------------------------------------------
def layout():
    events = load_events()
    strategies = load_strategies()

    event_types = sorted(events["event_type"].unique()) if not events.empty else []

    return html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Event Type"),
                            dcc.Dropdown(
                                id="ev-type-dropdown",
                                options=[{"label": e, "value": e} for e in event_types],
                                value=event_types[0] if event_types else None,
                                clearable=False,
                            ),
                        ],
                        width=3,
                    ),
                    dbc.Col(
                        [
                            html.Label("Strategy"),
                            dcc.Dropdown(
                                id="ev-strategy-dropdown",
                                options=[
                                    {"label": row["label"], "value": row["id"]}
                                    for _, row in strategies.iterrows()
                                ],
                                value=strategies["id"].iloc[0] if not strategies.empty else None,
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
                    dbc.Col(dcc.Graph(id="ev-timeline-graph"), width=12),
                ],
                className="mb-4",
            ),

            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="ev-impact-graph"), width=12),
                ]
            ),
        ]
    )


# ------------------------------------------------------------
# Callbacks
# ------------------------------------------------------------
@callback(
    Output("ev-timeline-graph", "figure"),
    Output("ev-impact-graph", "figure"),
    Input("ev-type-dropdown", "value"),
    Input("ev-strategy-dropdown", "value"),
)
def update_events(event_type, strategy_id):
    events = load_events()
    pnl = load_strategy_pnl(strategy_id)

    # ------------------------------------------------------------
    # Timeline graph
    # ------------------------------------------------------------
    fig_timeline = go.Figure()

    if not events.empty:
        ev = events[events["event_type"] == event_type] if event_type else events

        fig_timeline.add_trace(
            go.Scatter(
                x=ev["date"],
                y=[1] * len(ev),
                mode="markers",
                marker=dict(size=10, color="orange"),
                text=ev["label"],
                name="Events",
            )
        )

        fig_timeline.update_layout(
            title=f"Event Timeline — {event_type}",
            xaxis_title="Date",
            yaxis=dict(showticklabels=False),
            hovermode="x unified",
        )

    # ------------------------------------------------------------
    # Event impact graph
    # ------------------------------------------------------------
    fig_impact = go.Figure()

    if not events.empty and not pnl.empty:
        ev = events[events["event_type"] == event_type] if event_type else events

        # Align events to strategy pnl range
        ev = ev[(ev["date"] >= pnl["date"].min()) & (ev["date"] <= pnl["date"].max())]

        fig_impact.add_trace(
            go.Scatter(
                x=pnl["date"],
                y=pnl["cumulative_pnl"],
                mode="lines",
                name="Cumulative PnL",
                line=dict(color="steelblue"),
            )
        )

        for _, row in ev.iterrows():
            fig_impact.add_vline(
                x=row["date"],
                line=dict(color="orange", width=1, dash="dot"),
                opacity=0.7,
            )

        fig_impact.update_layout(
            title=f"Event Impact on Strategy — {event_type}",
            xaxis_title="Date",
            yaxis_title="Cumulative PnL",
            hovermode="x unified",
        )

    return fig_timeline, fig_impact