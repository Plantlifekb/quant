import logging
from typing import Optional, Sequence

from quant.common.db import create_db_engine
from quant.engine.dag import DAG, build_dag
from quant.engine.tasks import get_task_registry

logger = logging.getLogger("quant.engine.orchestrator")


class Orchestrator:
    """
    Orchestrates execution of the quant pipeline DAG.
    """

    def __init__(self, engine=None, dag: Optional[DAG] = None):
        if engine is None:
            engine = create_db_engine()
        self.engine = engine
        self.dag = dag or build_dag(get_task_registry(), engine)

    def run_all(self) -> None:
        """
        Run the full DAG in dependency order.
        """
        logger.info("Starting full pipeline run...")
        self.dag.run_all()
        logger.info("Full pipeline run completed.")

    def run_task(self, task_name: str) -> None:
        """
        Run a single task (and its dependencies) by name.
        """
        logger.info("Running task: %s", task_name)
        self.dag.run_task(task_name)
        logger.info("Task %s completed.", task_name)

    def run_pipeline(self, tasks: Optional[Sequence[str]] = None) -> None:
        """
        Run a subset of tasks (and their dependencies) or the full DAG if tasks is None.
        """
        if tasks:
            logger.info("Running pipeline subset: %s", tasks)
            for t in tasks:
                self.dag.run_task(t)
            logger.info("Subset pipeline run completed.")
        else:
            self.run_all()


def run_all() -> None:
    """
    Convenience function: construct an Orchestrator and run the full DAG.
    """
    orchestrator = Orchestrator()
    orchestrator.run_all()


def run_task(task_name: str) -> None:
    """
    Convenience function: construct an Orchestrator and run a single task.
    """
    orchestrator = Orchestrator()
    orchestrator.run_task(task_name)


def run_pipeline(tasks: Optional[Sequence[str]] = None) -> None:
    """
    Convenience function: construct an Orchestrator and run a subset or full pipeline.
    """
    orchestrator = Orchestrator()
    orchestrator.run_pipeline(tasks)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_all()