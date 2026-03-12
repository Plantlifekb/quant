# quant/engine/__init__.py

from .db import create_db_engine

# Do NOT import anything from orchestrator here.
# The orchestrator module is executed directly by the container entrypoint.