import pandas as pd
from dash import html, dcc
import dash_bootstrap_components as dbc

from quant.infrastructure.db import get_engine


def layout():
    """
    Deterministic, DB-backed Research tab.
    Reads canonical research table.
    This tab is intentionally flexible — research output varies,
    but the structure remains deterministic.
    """
    df = load_research()

    if df.empty:
        return dbc.Alert(
            "No research data available.",
            color="warning",
            className="mt-3",
        )

    return dbc.Container(
        [
            html.H4("Research"),
            html.Hr(),

            # Chart: metric over time (generic research metric)
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(
                            figure=build_research_chart(df),
                            id="research-chart",
                        ),
                        width=12,
                    )
                ]
            ),

            # Table: latest 100 research rows
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Table.from_dataframe(
                            df.tail(100),
                            striped=True,
                            bordered=True,
                            hover=True,
                            size="sm",
                        ),
                        width=12,
                        className="mt-4",
                    )
                ]
            ),
        ],
        fluid=True,
        className="mt-3",
    )


def load_research() -> pd.DataFrame:
    """
    Canonical DB read.
    Assumes a table named 'research' with columns:
        timestamp (datetime)
        topic (text)
        metric (float)
        notes (text)
    """
    engine = get_engine()

    query = """
        SELECT
            timestamp,
            topic,
            metric,
            notes
        FROM research
        ORDER BY timestamp ASC
    """

    try:
        df = pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()

    return df


def build_research_chart(df: pd.DataFrame):
    """
    Deterministic Plotly figure for research metric.
    """
    import plotly.express as px

    fig = px.line(
        df,
        x="timestamp",
        y="metric",
        color="topic",
        title="Research Metrics Over Time",
    )

    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),
        legend_title_text="Topic",
    )

    return fig