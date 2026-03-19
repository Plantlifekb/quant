from dash import html, dcc
import dash_bootstrap_components as dbc
from dash import Input, Output
import pandas as pd

from quant.dashboard.db import get_engine


def layout():
    return html.Div(
        [
            html.H3("Orchestrator Cycle Summary", className="mb-4"),

            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("Last Cycle"),
                                dbc.CardBody(
                                    [
                                        html.Div(id="cycle-last-run"),
                                        html.Div(id="cycle-duration"),
                                        html.Div(id="cycle-status"),
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
                                dbc.CardHeader("Recent Cycle Durations"),
                                dbc.CardBody(
                                    [
                                        dcc.Loading(
                                            dcc.Graph(id="cycle-history-graph"),
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
        Output("cycle-last-run", "children"),
        Output("cycle-duration", "children"),
        Output("cycle-status", "children"),
        Output("cycle-history-graph", "figure"),
        Input("interval-component", "n_intervals"),
    )
    def update_cycle_panel(_):
        engine = get_engine()

        query = """
            SELECT
                timestamp,
                event_type,
                duration_ms,
                status
            FROM event_log
            WHERE event_type = 'cycle'
            ORDER BY timestamp DESC
            LIMIT 50;
        """

        df = pd.read_sql(query, engine)

        if df.empty:
            return (
                "Last Run: —",
                "Duration: —",
                "Status: —",
                {
                    "data": [],
                    "layout": {"title": "No cycle data"},
                },
            )

        last = df.iloc[0]

        fig = {
            "data": [
                {
                    "x": df["timestamp"],
                    "y": df["duration_ms"],
                    "type": "line",
                    "name": "Cycle Duration (ms)",
                }
            ],
            "layout": {
                "title": "Recent Cycle Durations",
                "xaxis": {"title": "Timestamp"},
                "yaxis": {"title": "Duration (ms)"},
            },
        }

        return (
            f"Last Run: {last['timestamp']}",
            f"Duration: {last['duration_ms']} ms",
            f"Status: {last['status']}",
            fig,
        )