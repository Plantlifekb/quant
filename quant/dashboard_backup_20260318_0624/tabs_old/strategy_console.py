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


def load_strategy_summary(strategy_id: int) -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT *
        FROM strategy_summary
        WHERE strategy_id = %s;
    """
    try:
        return pd.read_sql(query, engine, params=(strategy_id,))
    except Exception:
        return pd.DataFrame()


def load_strategy_metadata(strategy_id: int) -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT *
        FROM strategy_metadata
        WHERE strategy_id = %s;
    """
    try:
        return pd.read_sql(query, engine, params=(strategy_id,))
    except Exception:
        return pd.DataFrame()


def load_strategy_metrics(strategy_id: int) -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT *
        FROM strategy_metrics_materialized
        WHERE strategy_id = %s;
    """
    try:
        return pd.read_sql(query, engine, params=(strategy_id,))
    except Exception:
        return pd.DataFrame()


def load_strategy_risk(strategy_id: int) -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT *
        FROM strategy_risk
        WHERE strategy_id = %s;
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
            html.Div(
                [
                    html.Label("Strategy"),
                    dcc.Dropdown(
                        id="sc-strategy-dropdown",
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

            dbc.Row(
                [
                    dbc.Col(dbc.Card(id="sc-summary-card"), width=4),
                    dbc.Col(dbc.Card(id="sc-metrics-card"), width=4),
                    dbc.Col(dbc.Card(id="sc-risk-card"), width=4),
                ],
                className="mb-4",
            ),

            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="sc-metrics-graph"), width=12),
                ]
            ),
        ]
    )


# ------------------------------------------------------------
# Callbacks
# ------------------------------------------------------------
@callback(
    Output("sc-summary-card", "children"),
    Output("sc-metrics-card", "children"),
    Output("sc-risk-card", "children"),
    Output("sc-metrics-graph", "figure"),
    Input("sc-strategy-dropdown", "value"),
)
def update_strategy_console(strategy_id):
    if strategy_id is None:
        return (
            dbc.CardBody("No strategy selected"),
            dbc.CardBody("No metrics"),
            dbc.CardBody("No risk data"),
            go.Figure(),
        )

    summary = load_strategy_summary(strategy_id)
    metadata = load_strategy_metadata(strategy_id)
    metrics = load_strategy_metrics(strategy_id)
    risk = load_strategy_risk(strategy_id)

    # -------------------------
    # Summary card
    # -------------------------
    if summary.empty:
        summary_card = dbc.CardBody("No summary available")
    else:
        row = summary.iloc[0]
        summary_card = dbc.CardBody(
            [
                html.H5("Summary", className="card-title"),
                html.Div(f"Start Date: {row.get('start_date', 'N/A')}"),
                html.Div(f"End Date: {row.get('end_date', 'N/A')}"),
                html.Div(f"Total Return: {row.get('total_return', 'N/A')}"),
                html.Div(f"CAGR: {row.get('cagr', 'N/A')}"),
                html.Div(f"Max Drawdown: {row.get('max_drawdown', 'N/A')}"),
            ]
        )

    # -------------------------
    # Metrics card
    # -------------------------
    if metrics.empty:
        metrics_card = dbc.CardBody("No metrics available")
    else:
        row = metrics.iloc[0]
        metrics_card = dbc.CardBody(
            [
                html.H5("Metrics", className="card-title"),
                html.Div(f"Sharpe: {row.get('sharpe', 'N/A')}"),
                html.Div(f"Sortino: {row.get('sortino', 'N/A')}"),
                html.Div(f"Volatility: {row.get('volatility', 'N/A')}"),
                html.Div(f"Win Rate: {row.get('win_rate', 'N/A')}"),
                html.Div(f"Hit Rate: {row.get('hit_rate', 'N/A')}"),
            ]
        )

    # -------------------------
    # Risk card
    # -------------------------
    if risk.empty:
        risk_card = dbc.CardBody("No risk data available")
    else:
        row = risk.iloc[0]
        risk_card = dbc.CardBody(
            [
                html.H5("Risk", className="card-title"),
                html.Div(f"Beta: {row.get('beta', 'N/A')}"),
                html.Div(f"VaR: {row.get('var', 'N/A')}"),
                html.Div(f"CVaR: {row.get('cvar', 'N/A')}"),
                html.Div(f"Exposure: {row.get('exposure', 'N/A')}"),
            ]
        )

    # -------------------------
    # Metrics graph
    # -------------------------
    fig = go.Figure()

    if not metrics.empty:
        row = metrics.iloc[0]
        labels = ["Sharpe", "Sortino", "Volatility", "Win Rate", "Hit Rate"]
        values = [
            row.get("sharpe", 0),
            row.get("sortino", 0),
            row.get("volatility", 0),
            row.get("win_rate", 0),
            row.get("hit_rate", 0),
        ]

        fig.add_trace(
            go.Bar(
                x=labels,
                y=values,
                marker_color="steelblue",
            )
        )

        fig.update_layout(
            title="Strategy Metrics",
            yaxis_title="Value",
            margin=dict(l=40, r=20, t=60, b=40),
        )

    return summary_card, metrics_card, risk_card, fig