from dash import html, dcc
import plotly.express as px
import pandas as pd
from quant.common.db import create_db_engine


def layout():
    engine = create_db_engine()

    df = pd.read_sql(
        "SELECT ticker, date, ret FROM returns ORDER BY ticker, date",
        engine,
    )

    fig = px.line(
        df,
        x="date",
        y="ret",
        color="ticker",
        title="Returns",
    )

    return html.Div([
        html.H2("Returns"),
        dcc.Graph(figure=fig),
    ])