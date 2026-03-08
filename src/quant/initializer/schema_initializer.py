#!/usr/bin/env python3
import logging
import sys
from sqlalchemy import text
from quant.common.db import create_db_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("quant.initializer")

def main():
    try:
        engine = create_db_engine()
        with engine.begin() as conn:
            logger.info("Applying schema.sql...")
            with open("/app/quant/config/schema.sql", "r") as f:
                sql = f.read()
            conn.execute(text(sql))
            logger.info("Schema applied successfully")
    except Exception as e:
        logger.exception("Failed to initialize schema: %s", e)
        sys.exit(1)

if __name__ == "__main__":
    main()