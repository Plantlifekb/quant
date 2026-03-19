from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List

from sqlalchemy.engine import Engine


TaskFn = Callable[[Engine], None]


@dataclass
class DAG:
    tasks: Dict[str, TaskFn] = field(default_factory=dict)
    order: List[str] = field(default_factory=list)

    def add_task(self, name: str, fn: TaskFn) -> None:
        if name in self.tasks:
            raise ValueError(f"Task {name!r} already exists in DAG.")
        self.tasks[name] = fn
        self.order.append(name)


# Example placeholder tasks — replace with real ones
def task_example_load_data(engine: Engine) -> None:
    # Implement your data load logic here
    pass


def task_example_compute_signals(engine: Engine) -> None:
    # Implement your signal computation logic here
    pass


def task_example_persist_results(engine: Engine) -> None:
    # Implement your persistence logic here
    pass


def build_default_dag(engine: Engine) -> DAG:
    """
    Build the default orchestrator DAG.
    Replace placeholder tasks with real ones as needed.
    """
    dag = DAG()
    dag.add_task("load_data", task_example_load_data)
    dag.add_task("compute_signals", task_example_compute_signals)
    dag.add_task("persist_results", task_example_persist_results)
    return dag