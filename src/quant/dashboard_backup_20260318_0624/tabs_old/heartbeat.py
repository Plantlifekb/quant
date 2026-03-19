from dash import html, dcc
import dash_bootstrap_components as dbc
from dash import Input, Output
import pandas as pd
from datetime import datetime, timezone

from quant.dashboard.db import get_engine


def layout():
    return html.Div(
        [
            html.H3("System Heartbeat", className="mb-4"),

            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("Latest Heartbeat"),
                                dbc.CardBody(
                                    [
                                        html.Div(id="hb-latency"),
                                        html.Div(id="hb-status"),
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
                                dbc.CardHeader("Heartbeat Latency History"),
                                dbc.CardBody(
                                    [
                                        dcc.Loading(
                                            dcc.Graph(id="hb-history-graph"),
                                            type="dot",
                                        )
                                    ]
                                ),
                            ]
                        ),
                        width=8,
                    ),
                ]
            ),
        ]
    )


def register_callbacks(app):
    @app.callback(
        Output("hb-latency", "children"),
        Output("hb-status", "children"),
        Output("hb-history-graph", "figure"),
        Input("interval-component", "n_intervals"),
    )
    def update_heartbeat(_):
        engine = get_engine()

        query = """
            SELECT
                timestamp,
                event_type
            FROM event_log
            WHERE event_type = 'heartbeat'
            ORDER BY timestamp DESC
            LIMIT 50;
        """

        df = pd.read_sql(query, engine)

        if df.empty:
            return (
                "Latency: —",
                "Status: No heartbeat events",
                {
                    "data": [],
                    "layout": {"title": "No heartbeat data"},
                },
            )

        # Latest heartbeat
        last_ts = df.iloc[0]["timestamp"]

        now = datetime.now(timezone.utc)
        latency_sec = (now - last_ts).total_seconds()

        status = "OK" if latency_sec < 30 else "STALE"

        fig = {
            "data": [
                {
                    "x": df["timestamp"],
                    "y": (now - df["timestamp"]).dt.total_seconds(),
                    "type": "line",
                    "name": "Latency (sec)",
                }
            ],
            "layout": {
                "title": "Heartbeat Latency Over Time",
                "xaxis": {"title": "Timestamp"},
                "yaxis": {"title": "Latency (sec)"},
            },
        }

        return (
            f"Latency: {latency_sec:.2f} sec",
            f"Status: {status}",
            fig,
        )