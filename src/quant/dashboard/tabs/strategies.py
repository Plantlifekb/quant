from dash import html, dcc
import plotly.express as px
import pandas as pd
from quant.common.db import create_db_engine


def layout():
    engine = create_db_engine()

    df = pd.read_sql(
        "SELECT ticker, date, pnl FROM strategies ORDER BY ticker, date",
        engine,
    )

    fig = px.line(
        df,
        x="date",
        y="pnl",
        color="ticker",
        title="Strategy PnL",
    )

    return html.Div([
        html.H2("Strategies"),
        dcc.Graph(figure=fig),
    ])