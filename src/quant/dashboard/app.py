from dash import Dash
from quant.dashboard.layout import serve_layout
from quant.dashboard import callbacks
import os

def create_app():
    app = Dash(
        __name__,
        suppress_callback_exceptions=True,
        meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
    )
    app.title = "Quant Dashboard"
    app.layout = serve_layout()
    callbacks.register_callbacks(app)
    return app

app = create_app()
server = app.server

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8050"))
    app.run_server(host="0.0.0.0", port=port, debug=False)
