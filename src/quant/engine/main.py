"""
Main entrypoint for programmatic use of the quant engine.

Import‑safe: orchestrator is only imported inside main().
"""

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("quant.engine.main")


def main() -> None:
    from quant.engine.orchestrator import Orchestrator
    from quant.common.db import create_db_engine

    engine = create_db_engine()
    orchestrator = Orchestrator(engine)
    orchestrator.run_all()


if __name__ == "__main__":
    main()