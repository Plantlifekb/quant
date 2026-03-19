from dash import dcc
from .tabs import (
    returns,
    signals,
    strategies,
    positions,
    portfolio,
    risk,
    orders,
    executions,
    research,
)


def get_tabs():
    return dcc.Tabs(
        [
            dcc.Tab(label="Returns", value="returns"),
            dcc.Tab(label="Signals", value="signals"),
            dcc.Tab(label="Strategies", value="strategies"),
            dcc.Tab(label="Positions", value="positions"),
            dcc.Tab(label="Portfolio", value="portfolio"),
            dcc.Tab(label="Risk", value="risk"),
            dcc.Tab(label="Orders", value="orders"),
            dcc.Tab(label="Executions", value="executions"),
            dcc.Tab(label="Research", value="research"),
        ],
        id="tabs",
        value="returns",
    )