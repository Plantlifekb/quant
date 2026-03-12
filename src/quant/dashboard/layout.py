from dash import html, dcc
import dash_bootstrap_components as dbc

NAV_ITEMS = [
    {"label": "Overview", "id": "tab-overview"},
    {"label": "Prices", "id": "tab-prices"},
    {"label": "Returns", "id": "tab-returns"},
    {"label": "Signals", "id": "tab-signals"},
    {"label": "Strategies", "id": "tab-strategies"},
]

def serve_layout():
    nav = dbc.Nav(
        [dbc.NavItem(dbc.NavLink(item["label"], href="#", id=item["id"])) for item in NAV_ITEMS],
        pills=True,
        horizontal="center",
        class_name="mb-2",
    )

    header = dbc.Container(
        dbc.Row(
            [
                dbc.Col(html.H2("Quant Dashboard"), width=8),
                dbc.Col(html.Div(id="last-run", style={"textAlign": "right"}), width=4),
            ],
            align="center",
        ),
        fluid=True,
        className="py-2",
    )

    content = html.Div(id="page-content", style={"padding": "1rem"})

    return html.Div([dcc.Location(id="url"), nav, header, content])
