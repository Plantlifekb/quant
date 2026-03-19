from dash import html, dcc
import dash_bootstrap_components as dbc
from dash import Input, Output
import pandas as pd
from datetime import datetime, timezone

from quant.dashboard.db import get_engine


def layout():
    return html.Div(
        [
            html.H3("System Overview", className="mb-4"),

            dbc.Row(
                [
                    # --- Last Cycle ---
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("Last Cycle"),
                                dbc.CardBody(
                                    [
                                        html.Div(id="ov-last-cycle-ts"),
                                        html.Div(id="ov-last-cycle-duration"),
                                        html.Div(id="ov-last-cycle-status"),
                                    ]
                                ),
                            ],
                            className="mb-4",
                        ),
                        width=4,
                    ),

                    # --- Heartbeat ---
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("Heartbeat"),
                                dbc.CardBody(
                                    [
                                        html.Div(id="ov-hb-latency"),
                                        html.Div(id="ov-hb-status"),
                                    ]
                                ),
                            ],
                            className="mb-4",
                        ),
                        width=4,
                    ),

                    # --- Failures ---
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("Recent Failures"),
                                dbc.CardBody(
                                    [
                                        html.Div(id="ov-failure-count"),
                                        html.Div(id="ov-last-failure"),
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
                                dbc.CardHeader("Event Severity Counts"),
                                dbc.CardBody(
                                    [
                                        html.Div(id="ov-sev-info"),
                                        html.Div(id="ov-sev-warning"),
                                        html.Div(id="ov-sev-error"),
                                    ]
                                ),
                            ]
                        ),
                        width=4,
                    ),

                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader("Cycle Duration Trend"),
                                dbc.CardBody(
                                    [
                                        dcc.Loading(
                                            dcc.Graph(id="ov-cycle-trend"),
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
        Output("ov-last-cycle-ts", "children"),
        Output("ov-last-cycle-duration", "children"),
        Output("ov-last-cycle-status", "children"),
        Output("ov-hb-latency", "children"),
        Output("ov-hb-status", "children"),
        Output("ov-failure-count", "children"),
        Output("ov-last-failure", "children"),
        Output("ov-sev-info", "children"),
        Output("ov-sev-warning", "children"),
        Output("ov-sev-error", "children"),
        Output("ov-cycle-trend", "figure"),
        Input("interval-component", "n_intervals"),
    )
    def update_overview(_):
        engine = get_engine()

        query = """
            SELECT
                timestamp,
                event_type,
                severity,
                duration_ms,
                status,
                message
            FROM event_log
            ORDER BY timestamp DESC
            LIMIT 500;
        """

        df = pd.read_sql(query, engine)

        # -------------------------
        # Last Cycle
        # -------------------------
        cycles = df[df["event_type"] == "cycle"]
        if not cycles.empty:
            last_cycle = cycles.iloc[0]
            last_cycle_ts = f"Timestamp: {last_cycle['timestamp']}"
            last_cycle_dur = f"Duration: {last_cycle['duration_ms']} ms"
            last_cycle_status = f"Status: {last_cycle['status']}"
        else:
            last_cycle_ts = "Timestamp: —"
            last_cycle_dur = "Duration: —"
            last_cycle_status = "Status: —"

        # -------------------------
        # Heartbeat
        # -------------------------
        heartbeats = df[df["event_type"] == "heartbeat"]
        if not heartbeats.empty:
            last_hb = heartbeats.iloc[0]["timestamp"]
            now = datetime.now(timezone.utc)
            latency = (now - last_hb).total_seconds()
            hb_latency = f"Latency: {latency:.2f} sec"
            hb_status = "Status: OK" if latency < 30 else "Status: STALE"
        else:
            hb_latency = "Latency: —"
            hb_status = "Status: No heartbeat"

        # -------------------------
        # Failures
        # -------------------------
        failures = df[df["severity"] == "error"]
        failure_count = f"Failures (recent): {len(failures)}"

        if not failures.empty:
            last_fail = failures.iloc[0]
            last_failure = f"Last Failure: {last_fail['timestamp']} | {last_fail['message']}"
        else:
            last_failure = "Last Failure: —"

        # -------------------------
        # Severity counts
        # -------------------------
        sev_info = f"Info: {(df['severity'] == 'info').sum()}"
        sev_warn = f"Warning: {(df['severity'] == 'warning').sum()}"
        sev_error = f"Error: {(df['severity'] == 'error').sum()}"

        # -------------------------
        # Cycle trend graph
        # -------------------------
        if not cycles.empty:
            fig = {
                "data": [
                    {
                        "x": cycles["timestamp"],
                        "y": cycles["duration_ms"],
                        "type": "line",
                        "name": "Cycle Duration (ms)",
                    }
                ],
                "layout": {
                    "title": "Cycle Duration Trend",
                    "xaxis": {"title": "Timestamp"},
                    "yaxis": {"title": "Duration (ms)"},
                },
            }
        else:
            fig = {"data": [], "layout": {"title": "No cycle data"}}

        return (
            last_cycle_ts,
            last_cycle_dur,
            last_cycle_status,
            hb_latency,
            hb_status,
            failure_count,
            last_failure,
            sev_info,
            sev_warn,
            sev_error,
            fig,
        )