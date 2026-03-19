import pandas as pd
from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from quant.infrastructure.db import get_engine


# ------------------------------------------------------------
# DB loaders
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


def load_positions(strategy_id: int) -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT
            date,
            ticker,
            position,
            exposure
        FROM strategy_positions_daily
        WHERE strategy_id = %s
        ORDER BY date, ticker;
    """
    try:
        return pd.read_sql(query, engine, params=(strategy_id,))
    except Exception:
        return pd.DataFrame()


# ------------------------------------------------------------
# Layout
# ------------------------------------------------------------
def layout():
    strategies = load_strategies()

    return html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Strategy"),
                            dcc.Dropdown(
                                id="px-strategy-dropdown",
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
                    dbc.Col(
                        [
                            html.Label("Ticker"),
                            dcc.Dropdown(
                                id="px-ticker-dropdown",
                                options=[],
                                value=None,
                                clearable=True,
                            ),
                        ],
                        width=3,
                    ),
                ],
                className="mb-4",
            ),

            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="px-position-graph"), width=12),
                ],
                className="mb-4",
            ),

            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="px-exposure-graph"), width=12),
                ],
                className="mb-4",
            ),

            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="px-heatmap-graph"), width=12),
                ]
            ),
        ]
    )


# ------------------------------------------------------------
# Callbacks
# ------------------------------------------------------------
@callback(
    Output("px-ticker-dropdown", "options"),
    Output("px-ticker-dropdown", "value"),
    Input("px-strategy-dropdown", "value"),
)
def update_ticker_list(strategy_id):
    df = load_positions(strategy_id)
    if df.empty:
        return [], None

    tickers = sorted(df["ticker"].unique())
    return [{"label": t, "value": t} for t in tickers], None


@callback(
    Output("px-position-graph", "figure"),
    Output("px-exposure-graph", "figure"),
    Output("px-heatmap-graph", "figure"),
    Input("px-strategy-dropdown", "value"),
    Input("px-ticker-dropdown", "value"),
)
def update_positions(strategy_id, ticker):
    df = load_positions(strategy_id)

    # ------------------------------------------------------------
    # Position graph
    # ------------------------------------------------------------
    fig_pos = go.Figure()

    if not df.empty:
        if ticker:
            sub = df[df["ticker"] == ticker]
            fig_pos.add_trace(
                go.Scatter(
                    x=sub["date"],
                    y=sub["position"],
                    mode="lines",
                    name=f"{ticker} Position",
                    line=dict(color="steelblue"),
                )
            )
        else:
            fig_pos.add_trace(
                go.Scatter(
                    x=df["date"],
                    y=df.groupby("date")["position"].sum(),
                    mode="lines",
                    name="Total Position",
                    line=dict(color="steelblue"),
                )
            )

        fig_pos.update_layout(
            title="Positions Over Time",
            xaxis_title="Date",
            yaxis_title="Position",
            hovermode="x unified",
        )

    # ------------------------------------------------------------
    # Exposure graph
    # ------------------------------------------------------------
    fig_exp = go.Figure()

    if not df.empty:
        if ticker:
            sub = df[df["ticker"] == ticker]
            fig_exp.add_trace(
                go.Scatter(
                    x=sub["date"],
                    y=sub["exposure"],
                    mode="lines",
                    name=f"{ticker} Exposure",
                    line=dict(color="darkorange"),
                )
            )
        else:
            fig_exp.add_trace(
                go.Scatter(
                    x=df["date"],
                    y=df.groupby("date")["exposure"].sum(),
                    mode="lines",
                    name="Total Exposure",
                    line=dict(color="darkorange"),
                )
            )

        fig_exp.update_layout(
            title="Exposure Over Time",
            xaxis_title="Date",
            yaxis_title="Exposure",
            hovermode="x unified",
        )

    # ------------------------------------------------------------
    # Heatmap (tickers × dates)
    # ------------------------------------------------------------
    fig_heat = go.Figure()

    if not df.empty:
        pivot = df.pivot(index="ticker", columns="date", values="position").fillna(0)

        fig_heat.add_trace(
            go.Heatmap(
                z=pivot.values,
                x=pivot.columns,
                y=pivot.index,
                colorscale="Viridis",
                colorbar=dict(title="Position"),
            )
        )

        fig_heat.update_layout(
            title="Position Heatmap (Ticker × Date)",
            xaxis_title="Date",
            yaxis_title="Ticker",
        )

    return fig_pos, fig_exp, fig_heat