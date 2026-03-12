import logging
from typing import Callable, Dict, List, Set

logger = logging.getLogger("quant.engine.dag")


class DAG:
    """
    Minimal deterministic DAG executor. Each task is a callable that accepts a DB engine.
    """

    def __init__(self, tasks: Dict[str, Callable], dependencies: Dict[str, List[str]], engine):
        self.tasks = tasks
        self.dependencies = dependencies
        self.engine = engine

    def _resolve(self, task: str, visited: Set[str], order: List[str]) -> None:
        if task in visited:
            return
        visited.add(task)

        for dep in self.dependencies.get(task, []):
            self._resolve(dep, visited, order)

        order.append(task)

    def _topological_sort(self, root: str) -> List[str]:
        visited: Set[str] = set()
        order: List[str] = []
        self._resolve(root, visited, order)
        return order

    def run_task(self, task_name: str) -> None:
        order = self._topological_sort(task_name)
        logger.info("Execution order for %s: %s", task_name, order)

        for t in order:
            logger.info("Running task: %s", t)
            try:
                self.tasks[t](self.engine)
            except Exception as e:
                logger.exception("Task %s FAILED: %s", t, e)
                # Fail fast — stop the DAG and surface the error to the caller/orchestrator
                raise

    def run_all(self) -> None:
        """
        Run all tasks in dependency order.
        """
        visited: Set[str] = set()
        order: List[str] = []

        for task in self.tasks:
            self._resolve(task, visited, order)

        logger.info("Full DAG execution order: %s", order)

        for t in order:
            logger.info("Running task: %s", t)
            try:
                self.tasks[t](self.engine)
            except Exception as e:
                logger.exception("Task %s FAILED: %s", t, e)
                raise


def build_dag(task_registry: Dict[str, Callable], engine):
    """
    Build a DAG from the task registry.
    Each task may define a `.dependencies` attribute (list of task names).
    """
    dependencies = {
        name: getattr(func, "dependencies", [])
        for name, func in task_registry.items()
    }
    return DAG(task_registry, dependencies, engine)