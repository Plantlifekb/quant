# quant/engine/db.py

import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def create_db_engine() -> Engine:
    """
    Creates a SQLAlchemy engine using environment variables.
    Deterministic, container‑safe, and used by all engine components.
    """

    db_user = os.getenv("POSTGRES_USER", "quant")
    db_pass = os.getenv("POSTGRES_PASSWORD", "quant")
    db_host = os.getenv("POSTGRES_HOST", "db")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "quant")

    url = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

    engine = create_engine(
        url,
        echo=False,
        future=True,
        pool_pre_ping=True,
    )

    return engine