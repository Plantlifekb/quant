from dash import html, dcc

from quant.dashboard.tabs.tab1 import tab1_layout
from quant.dashboard.tabs.tab_market_conditions import market_conditions_layout
from quant.dashboard.tabs.tab_signals import signals_layout
from quant.dashboard.tabs.tab_strategies import strategies_layout


layout = html.Div(
    [
        dcc.Tabs(
            id="main-tabs",
            value="tab1",
            children=[
                dcc.Tab(label="Overview", value="tab1"),
                dcc.Tab(label="Market Conditions", value="market_conditions"),
                dcc.Tab(label="Signals", value="signals"),
                dcc.Tab(label="Strategies", value="strategies"),
            ],
        ),
        html.Div(id="tab-content"),
    ]
)