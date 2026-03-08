#!/usr/bin/env python3
# quant.common.db

import os
import logging
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

logger = logging.getLogger("db")


@lru_cache(maxsize=1)
def create_db_engine() -> Engine:
    """
    Create and cache a SQLAlchemy engine using the DATABASE_URL environment variable.

    This is the single, authoritative way to obtain a DB engine for:
      - the orchestrator (quant.engine)
      - the dashboard (quant.dashboard)
      - analytics, signals, strategies, and any future quant services

    The engine is created once per process and reused, ensuring consistent
    configuration and avoiding unnecessary connection churn.
    """
    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        raise RuntimeError(
            "DATABASE_URL is not set. "
            "Set it in your environment or Docker Compose configuration."
        )

    logger.info(f"Creating DB engine for: {db_url}")

    try:
        engine = create_engine(
            db_url,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
        )
        logger.info("DB engine created successfully.")
        return engine
    except Exception as e:
        logger.error(f"Failed to create DB engine: {e}")
        raise