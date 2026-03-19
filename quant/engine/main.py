"""
Main entrypoint for programmatic use of the quant engine.

Import‑safe: orchestrator is only imported inside main().
"""

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("quant.engine.main")


def main() -> None:
    from quant.common.db import create_db_engine
    from quant.engine.orchestrator import (
        run_cycle,
        write_cycle_to_db,
        write_steps_to_db,
    )

    engine = create_db_engine()

    logger.info("Starting orchestrator cycle...")
    cycle = run_cycle(engine)

    logger.info(f"Cycle complete: {cycle.cycle_id} ({cycle.status})")

    # Persist results
    write_cycle_to_db(engine, cycle)
    write_steps_to_db(engine, cycle)

    logger.info("Cycle + steps written to database.")


if __name__ == "__main__":
    main()