from dash import html, dcc
import dash_bootstrap_components as dbc
from dash import Input, Output
import pandas as pd

from quant.dashboard.db import get_engine


def layout():
    return html.Div(
        [
            html.H3("Prices", className="mb-4"),

            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Select Ticker:"),
                            dcc.Dropdown(
                                id="prices-ticker-dropdown",
                                options=[],
                                value=None,
                                clearable=False,
                                style={"width": "250px"},
                            ),
                        ],
                        width=3,
                    ),
                    dbc.Col(
                        [
                            html.Div(id="prices-last-updated"),
                        ],
                        width=9,
                    ),
                ],
                className="mb-4",
            ),

            dcc.Loading(
                dcc.Graph(id="prices-chart"),
                type="dot",
            ),
        ]
    )


def register_callbacks(app):
    @app.callback(
        Output("prices-ticker-dropdown", "options"),
        Output("prices-ticker-dropdown", "value"),
        Input("interval-component", "n_intervals"),
    )
    def load_tickers(_):
        engine = get_engine()

        query = """
            SELECT DISTINCT symbol
            FROM prices
            ORDER BY symbol ASC;
        """

        df = pd.read_sql(query, engine)

        if df.empty:
            return [], None

        options = [{"label": s, "value": s} for s in df["symbol"]]
        default = df["symbol"].iloc[0]

        return options, default

    @app.callback(
        Output("prices-chart", "figure"),
        Output("prices-last-updated", "children"),
        Input("prices-ticker-dropdown", "value"),
        Input("interval-component", "n_intervals"),
    )
    def update_price_chart(symbol, _):
        if symbol is None:
            return {"data": [], "layout": {"title": "No ticker selected"}}, ""

        engine = get_engine()

        query = """
            SELECT
                timestamp,
                symbol,
                price
            FROM prices
            WHERE symbol = %s
            ORDER BY timestamp ASC;
        """

        df = pd.read_sql(query, engine, params=[symbol])

        if df.empty:
            return (
                {"data": [], "layout": {"title": f"No price data for {symbol}"}},
                "",
            )

        fig = {
            "data": [
                {
                    "x": df["timestamp"],
                    "y": df["price"],
                    "type": "line",
                    "name": symbol,
                }
            ],
            "layout": {
                "title": f"Price History: {symbol}",
                "xaxis": {"title": "Timestamp"},
                "yaxis": {"title": "Price"},
            },
        }

        last_ts = df["timestamp"].iloc[-1]
        last_updated = f"Last Updated: {last_ts}"

        return fig, last_updated