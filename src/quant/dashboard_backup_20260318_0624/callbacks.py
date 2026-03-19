from dash import html, Input, Output
import dash_bootstrap_components as dbc

from . import dashboard_data


def register_callbacks(app):

    # ---------------------------------------------------------
    # TAB ROUTER
    # ---------------------------------------------------------
    @app.callback(
        Output("tab-content", "children"),
        Input("main-tabs", "value"),
    )
    def render_tab(active_tab: str):
        """
        Dynamically loads the layout for the selected tab.
        """
        module_map = {
            "tab-returns": "returns",
            "tab-signals": "signals",
            "tab-strategies": "strategies",
            "tab-positions": "positions",
            "tab-portfolio": "portfolio",
            "tab-risk": "risk",
            "tab-orders": "orders",
            "tab-executions": "executions",
            "tab-research": "research",
            "tab-hindsight": "hindsight",
            "tab-strategy-console": "strategy_console",
            "tab-market-context": "market_context",
            "tab-events": "events",
            "tab-positions-explorer": "positions_explorer",
            "tab-portfolio-console": "portfolio_console",
            "tab-risk-console": "risk_console",
            "tab-executions-console": "executions_console",
            "tab-research-workspace": "research_workspace",
        }

        if active_tab not in module_map:
            return html.Div("Unknown tab")

        module_name = module_map[active_tab]
        module = __import__(
            f"quant.dashboard.tabs.{module_name}",
            fromlist=["layout"]
        )
        return module.layout()

    # ---------------------------------------------------------
    # HEARTBEAT PANEL
    # ---------------------------------------------------------
    @app.callback(
        Output("heartbeat-panel", "children"),
        Input("main-tabs", "value"),
    )
    def update_heartbeat(_):
        df = dashboard_data.fetch_heartbeat()

        if df.empty:
            return dbc.Alert(
                "No heartbeat detected",
                color="danger",
                className="py-2 mb-2",
            )

        row = df.iloc[0]
        color = (
            "success" if row["status"] == "ok"
            else "warning" if row["status"] == "degraded"
            else "danger"
        )

        msg = (
            f"Last cycle: {row['cycle_name']} | "
            f"Status: {row['status']} | "
            f"Duration: {row['duration_ms']} ms | "
            f"Timestamp: {row['timestamp']}"
        )

        return dbc.Alert(msg, color=color, className="py-2 mb-2")

    # ---------------------------------------------------------
    # LATENCY PANEL
    # ---------------------------------------------------------
    @app.callback(
        Output("latency-panel", "children"),
        Input("main-tabs", "value"),
    )
    def update_latency(_):
        df = dashboard_data.fetch_latency(limit=10)

        if df.empty:
            return dbc.Alert(
                "No latency data available",
                color="warning",
                className="py-2 mb-2",
            )

        table = dbc.Table.from_dataframe(
            df.sort_values("timestamp", ascending=False),
            striped=True,
            bordered=True,
            hover=True,
            size="sm",
        )

        return dbc.Card(
            [
                dbc.CardHeader("Latency (recent steps)"),
                dbc.CardBody(table),
            ],
            className="mb-0",
        )

    # ---------------------------------------------------------
    # CYCLE PANEL
    # ---------------------------------------------------------
    @app.callback(
        Output("cycle-panel", "children"),
        Input("main-tabs", "value"),
    )
    def update_cycles(_):
        df = dashboard_data.fetch_cycle_summary(limit=10)

        if df.empty:
            return dbc.Alert(
                "No cycle data available",
                color="warning",
                className="py-2 mb-3",
            )

        table = dbc.Table.from_dataframe(
            df.sort_values("timestamp", ascending=False),
            striped=True,
            bordered=True,
            hover=True,
            size="sm",
        )

        return dbc.Card(
            [
                dbc.CardHeader("Cycles (recent)"),
            dbc.CardBody(table),
            ],
            className="mb-0",
        )