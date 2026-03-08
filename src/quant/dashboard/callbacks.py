from dash import Input, Output
from quant.dashboard.tabs.tab1_callbacks import render_tab1
from quant.dashboard.tabs.tab_market_conditions import render_market_conditions
from quant.dashboard.tabs.tab_signals import render_signals
from quant.dashboard.tabs.tab_strategies import render_strategies


def register_callbacks(app):
    @app.callback(
        Output("tab-content", "children"),
        Input("main-tabs", "value"),
    )
    def display_tab(tab):
        if tab == "tab1":
            return render_tab1()
        if tab == "market_conditions":
            return render_market_conditions()
        if tab == "signals":
            return render_signals()
        if tab == "strategies":
            return render_strategies()
        return "Unknown tab"