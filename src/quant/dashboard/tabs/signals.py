from dash import html, dcc
import plotly.express as px
import pandas as pd
from quant.common.db import create_db_engine


def layout():
    engine = create_db_engine()

    df = pd.read_sql(
        "SELECT ticker, date, signal FROM signals ORDER BY ticker, date",
        engine,
    )

    fig = px.line(
        df,
        x="date",
        y="signal",
        color="ticker",
        title="Signals",
    )

    return html.Div([
        html.H2("Signals"),
        dcc.Graph(figure=fig),
    ])