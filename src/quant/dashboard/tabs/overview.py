from dash import html
from sqlalchemy import text
from quant.common.db import create_db_engine

def layout():
    engine = create_db_engine()

    with engine.begin() as conn:
        total_pnl = conn.execute(
            text("SELECT value FROM dashboard_summary WHERE metric='total_pnl'")
        ).scalar() or 0.0

    return html.Div([
        html.H2("Overview"),
        html.H3(f"Total PnL: {total_pnl:.4f}")
    ])