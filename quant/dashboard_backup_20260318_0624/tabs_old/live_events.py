from dash import html, dcc
import dash_bootstrap_components as dbc
from dash import Input, Output
import pandas as pd

from quant.dashboard.db import get_engine


def layout():
    return html.Div(
        [
            html.H3("Live Orchestrator Events", className="mb-3"),

            dcc.Interval(
                id="events-interval",
                interval=2_000,
                n_intervals=0,
            ),

            html.Div(
                [
                    html.Label("Severity:"),
                    dcc.Dropdown(
                        id="events-severity-filter",
                        options=[
                            {"label": "All", "value": "all"},
                            {"label": "Info", "value": "info"},
                            {"label": "Warning", "value": "warning"},
                            {"label": "Error", "value": "error"},
                        ],
                        value="all",
                        clearable=False,
                        style={"width": "200px"},
                    ),
                ],
                style={"marginBottom": "10px"},
            ),

            html.Div(
                id="events-log",
                style={
                    "height": "400px",
                    "overflowY": "scroll",
                    "fontFamily": "monospace",
                    "fontSize": "12px",
                    "border": "1px solid #ccc",
                    "padding": "5px",
                    "backgroundColor": "#111",
                    "color": "#eee",
                },
            ),
        ]
    )


def register_callbacks(app):
    @app.callback(
        Output("events-log", "children"),
        Input("events-interval", "n_intervals"),
        Input("events-severity-filter", "value"),
    )
    def update_events(_, severity_filter):
        engine = get_engine()

        query = """
            SELECT
                timestamp,
                event_type,
                severity,
                message,
                duration_ms,
                status
            FROM event_log
            ORDER BY timestamp DESC
            LIMIT 200;
        """

        df = pd.read_sql(query, engine)

        if df.empty:
            return "No events recorded."

        # Apply severity filter
        if severity_filter != "all":
            df = df[df["severity"] == severity_filter]

        if df.empty:
            return f"No events with severity '{severity_filter}'."

        # Build log lines
        lines = []
        for _, row in df.iterrows():
            ts = row["timestamp"]
            et = row["event_type"]
            sev = row["severity"]
            msg = row.get("message", "")
            dur = row.get("duration_ms", None)
            status = row.get("status", "")

            dur_str = f" | {dur}ms" if dur is not None else ""
            status_str = f" | {status}" if status else ""

            line = f"{ts} | {sev.upper():7} | {et}{dur_str}{status_str} | {msg}"
            lines.append(line)

        return html.Pre("\n".join(lines))