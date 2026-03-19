from dash import Input, Output, html
import plotly.graph_objs as go
import plotly.express as px
from .dashboard_data import (
    get_price_series,
    get_signal_series,
    get_cycle_timeline,
    get_cycle_history,
    get_step_latency_history,
    get_failure_graph,
    get_failure_heatmap,
    get_anomaly_series,
    get_recent_events,
)


def register_data_callbacks(app):

    @app.callback(
        Output("price-chart", "figure"),
        Output("signal-chart", "figure"),
        Input("tabs", "value"),
    )
    def update_metrics(tab):
        if tab != "metrics":
            return {}, {}
        x_prices, prices = get_price_series()
        x_signals, signals = get_signal_series()
        return (
            go.Figure(
                data=[go.Scatter(x=x_prices, y=prices, mode="lines", name="Price")]
            ),
            go.Figure(
                data=[go.Scatter(x=x_signals, y=signals, mode="lines", name="Signal")]
            ),
        )

    @app.callback(
        Output("cycle-timeline-table", "data"),
        Input("tabs", "value"),
    )
    def update_cycle_timeline(tab):
        if tab != "timeline":
            return []
        rows = get_cycle_timeline()
        return [
            {
                "step": r["step"],
                "start": r["start_dt"].strftime("%H:%M:%S"),
                "end": r["end_dt"].strftime("%H:%M:%S"),
                "duration": r["duration"],
                "status": r["status"],
            }
            for r in rows
        ]

    @app.callback(
        Output("cycle-gantt-chart", "figure"),
        Input("tabs", "value"),
    )
    def update_cycle_gantt(tab):
        if tab != "gantt":
            return {}
        rows = get_cycle_timeline()
        fig = go.Figure()
        for r in rows:
            fig.add_trace(
                go.Bar(
                    x=[(r["end_dt"] - r["start_dt"]).total_seconds()],
                    y=[r["step"]],
                    base=[r["start_dt"]],
                    orientation="h",
                    name=r["step"],
                )
            )
        fig.update_layout(
            title="Cycle Gantt Timeline",
            barmode="stack",
            xaxis_title="Time",
            yaxis_title="Step",
        )
        return fig

    @app.callback(
        Output("cycle-latency-chart", "figure"),
        Input("tabs", "value"),
    )
    def update_cycle_latency(tab):
        if tab != "latency":
            return {}
        history = get_cycle_history()
        return go.Figure(
            data=[
                go.Scatter(
                    x=[h["timestamp"] for h in history],
                    y=[h["duration"] for h in history],
                    mode="lines+markers",
                    name="Cycle Duration",
                )
            ],
            layout=go.Layout(
                title="Cycle Duration Over Time",
                xaxis_title="Timestamp",
                yaxis_title="Duration (s)",
            ),
        )

    @app.callback(
        Output("step-latency-chart", "figure"),
        Input("tabs", "value"),
    )
    def update_step_latency(tab):
        if tab != "latency":
            return {}
        history = get_step_latency_history()
        steps = [k for k in history[0].keys() if k != "timestamp"]
        fig = go.Figure()
        for step in steps:
            fig.add_trace(
                go.Scatter(
                    x=[h["timestamp"] for h in history],
                    y=[h[step] for h in history],
                    mode="lines+markers",
                    name=step,
                )
            )
        fig.update_layout(
            title="Step Latency Trends",
            xaxis_title="Timestamp",
            yaxis_title="Duration (s)",
        )
        return fig

    @app.callback(
        Output("failure-graph", "figure"),
        Input("tabs", "value"),
    )
    def update_failure_graph(tab):
        if tab != "failures":
            return {}
        graph = get_failure_graph()
        nodes = graph["nodes"]
        fig = go.Figure()
        for idx, node in enumerate(nodes):
            fig.add_trace(
                go.Scatter(
                    x=[idx],
                    y=[0],
                    mode="markers+text",
                    text=[node["id"]],
                    textposition="top center",
                    marker=dict(
                        size=20,
                        color="green" if node["status"] == "SUCCESS" else "red",
                    ),
                    showlegend=False,
                )
            )
        fig.update_layout(
            title="Failure Path (Placeholder Layout)",
            xaxis_visible=False,
            yaxis_visible=False,
        )
        return fig

    @app.callback(
        Output("failure-heatmap", "figure"),
        Input("tabs", "value"),
    )
    def update_failure_heatmap(tab):
        if tab != "failures":
            return {}
        steps, cycles, matrix = get_failure_heatmap()
        fig = px.imshow(
            matrix,
            x=cycles,
            y=steps,
            color_continuous_scale="Reds",
        )
        fig.update_layout(title="Failure Heatmap")
        return fig

    @app.callback(
        Output("anomaly-chart", "figure"),
        Input("tabs", "value"),
    )
    def update_anomaly_chart(tab):
        if tab != "failures":
            return {}
        ts, anomalies = get_anomaly_series()
        fig = go.Figure(
            data=[
                go.Scatter(
                    x=ts,
                    y=anomalies,
                    mode="lines+markers",
                    name="Anomaly Score",
                )
            ],
            layout=go.Layout(
                title="Cycle Anomaly Scores",
                xaxis_title="Timestamp",
                yaxis_title="Z-score",
            ),
        )
        return fig

    @app.callback(
        Output("events-log", "children"),
        Input("events-interval", "n_intervals"),
        Input("events-severity-filter", "value"),
    )
    def update_events_log(n_intervals, severity_filter):
        events = get_recent_events(severity_filter=severity_filter, window_minutes=30)

        children = []
        for e in events:
            ts = e["timestamp"].strftime("%H:%M:%S")
            sev = e["severity"].upper()
            step = e["step"] or "-"
            msg = e["message"] or ""
            line = f"[{ts}] [{sev}] [{step}] {msg}"

            color = "#aaa"
            if sev == "ERROR":
                color = "#ff5555"
            elif sev == "WARNING":
                color = "#ffcc00"

            children.append(html.Div(line, style={"color": color}))

        return children