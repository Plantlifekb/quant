import dash
from dash import html, dcc
from quant.common.db import create_db_engine

from quant.dashboard.tabs.overview import layout as overview_layout
from quant.dashboard.tabs.prices import layout as prices_layout
from quant.dashboard.tabs.returns import layout as returns_layout
from quant.dashboard.tabs.signals import layout as signals_layout
from quant.dashboard.tabs.strategies import layout as strategies_layout

def create_app():
    app = dash.Dash(__name__)
    app.title = "Quant Dashboard"

    app.layout = html.Div([
        html.H1("Quant Dashboard"),

        dcc.Tabs(id="tabs", value="overview", children=[
            dcc.Tab(label="Overview", value="overview"),
            dcc.Tab(label="Prices", value="prices"),
            dcc.Tab(label="Returns", value="returns"),
            dcc.Tab(label="Signals", value="signals"),
            dcc.Tab(label="Strategies", value="strategies"),
        ]),

        html.Div(id="tab-content")
    ])

    @app.callback(
        dash.Output("tab-content", "children"),
        dash.Input("tabs", "value")
    )
    def render_tab(tab):
        if tab == "overview":
            return overview_layout()
        if tab == "prices":
            return prices_layout()
        if tab == "returns":
            return returns_layout()
        if tab == "signals":
            return signals_layout()
        if tab == "strategies":
            return strategies_layout()

    return app


def main():
    app = create_app()
    app.run_server(host="0.0.0.0", port=8050)


if __name__ == "__main__":
    main()