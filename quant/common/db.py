from __future__ import annotations

import os
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL environment variable is not set. "
            "Example: postgresql://quant:quant@db:5432/quant"
        )
    return url


_engine: Optional[Engine] = None


def create_db_engine() -> Engine:
    """
    Create (or return cached) SQLAlchemy engine using DATABASE_URL.
    """
    global _engine
    if _engine is None:
        url = get_database_url()
        _engine = create_engine(
            url,
            pool_pre_ping=True,
            future=True,
        )
    return _engine