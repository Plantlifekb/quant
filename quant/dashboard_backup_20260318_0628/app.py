import dash
from dash import Dash
from .layout import layout
from .callbacks import register_callbacks

def create_app():
    app = Dash(
        __name__,
        suppress_callback_exceptions=True,
        title="Quant Dashboard"
    )
    app.layout = layout
    register_callbacks(app)
    return app

def run():
    app = create_app()
    app.run_server(host="0.0.0.0", port=8050, debug=False)