from dash import html, dcc
import dash_bootstrap_components as dbc
from dash import Input, Output
import pandas as pd
from datetime import datetime, timezone

from quant.dashboard.db import get_engine


def layout():
    return html.Div(
        [
            html.H3("Quant Metrics", className="mb-4"),

            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("Cycle Metrics"),
                                dbc.CardBody(
                                    [
                                        html.Div(id="metric-cycle-avg"),
                                        html.Div(id="metric-cycle-max"),
                                    ]
                                ),
                            ],
                            className="mb-4",
                        ),
                        width=4,
                    ),
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("Heartbeat Metrics"),
                                dbc.CardBody(
                                    [
                                        html.Div(id="metric-hb-avg"),
                                        html.Div(id="metric-hb-max"),
                                    ]
                                ),
                            ],
                            className="mb-4",
                        ),
                        width=4,
                    ),
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("Event Severity Counts"),
                                dbc.CardBody(
                                    [
                                        html.Div(id="metric-severity-info"),
                                        html.Div(id="metric-severity-warning"),
                                        html.Div(id="metric-severity-error"),
                                    ]
                                ),
                            ],
                            className="mb-4",
                        ),
                        width=4,
                    ),
                ]
            ),

            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("Cycle Duration Distribution"),
                                dbc.CardBody(
                                    [
                                        dcc.Loading(
                                            dcc.Graph(id="metric-cycle-dist"),
                                            type="dot",
                                        )
                                    ]
                                ),
                            ]
                        ),
                        width=12,
                    )
                ]
            ),
        ]
    )


def register_callbacks(app):
    @app.callback(
        Output("metric-cycle-avg", "children"),
        Output("metric-cycle-max", "children"),
        Output("metric-hb-avg", "children"),
        Output("metric-hb-max", "children"),
        Output("metric-severity-info", "children"),
        Output("metric-severity-warning", "children"),
        Output("metric-severity-error", "children"),
        Output("metric-cycle-dist", "figure"),
        Input("interval-component", "n_intervals"),
    )
    def update_metrics(_):
        engine = get_engine()

        query = """
            SELECT
                timestamp,
                event_type,
                severity,
                duration_ms
            FROM event_log
            ORDER BY timestamp DESC
            LIMIT 500;
        """

        df = pd.read_sql(query, engine)

        if df.empty:
            empty_fig = {"data": [], "layout": {"title": "No cycle data"}}
            return (
                "Avg Cycle Duration: —",
                "Max Cycle Duration: —",
                "Avg Heartbeat Latency: —",
                "Max Heartbeat Latency: —",
                "Info: 0",
                "Warning: 0",
                "Error: 0",
                empty_fig,
            )

        # --- Cycle metrics ---
        cycles = df[df["event_type"] == "cycle"]
        if not cycles.empty:
            avg_cycle = cycles["duration_ms"].mean()
            max_cycle = cycles["duration_ms"].max()
        else:
            avg_cycle = max_cycle = None

        # --- Heartbeat metrics ---
        heartbeats = df[df["event_type"] == "heartbeat"]
        if not heartbeats.empty:
            now = datetime.now(timezone.utc)
            hb_latencies = (now - heartbeats["timestamp"]).dt.total_seconds()
            avg_hb = hb_latencies.mean()
            max_hb = hb_latencies.max()
        else:
            avg_hb = max_hb = None

        # --- Severity counts ---
        info_count = (df["severity"] == "info").sum()
        warn_count = (df["severity"] == "warning").sum()
        error_count = (df["severity"] == "error").sum()

        # --- Cycle duration distribution graph ---
        if not cycles.empty:
            fig = {
                "data": [
                    {
                        "x": cycles["duration_ms"],
                        "type": "histogram",
                        "name": "Cycle Duration (ms)",
                    }
                ],
                "layout": {
                    "title": "Cycle Duration Distribution",
                    "xaxis": {"title": "Duration (ms)"},
                    "yaxis": {"title": "Count"},
                },
            }
        else:
            fig = {"data": [], "layout": {"title": "No cycle data"}}

        return (
            f"Avg Cycle Duration: {avg_cycle:.2f} ms" if avg_cycle else "Avg Cycle Duration: —",
            f"Max Cycle Duration: {max_cycle:.2f} ms" if max_cycle else "Max Cycle Duration: —",
            f"Avg Heartbeat Latency: {avg_hb:.2f} sec" if avg_hb else "Avg Heartbeat Latency: —",
            f"Max Heartbeat Latency: {max_hb:.2f} sec" if max_hb else "Max Heartbeat Latency: —",
            f"Info: {info_count}",
            f"Warning: {warn_count}",
            f"Error: {error_count}",
            fig,
        )