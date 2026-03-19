from dash import html, dcc
from .navigation import navbar
from .tabs import get_tabs

layout = html.Div(
    [
        navbar,
        dcc.Tabs(id="tabs", value="returns", children=get_tabs()),
        html.Div(id="tab-content")
    ]
)