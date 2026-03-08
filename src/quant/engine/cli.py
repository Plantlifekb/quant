import argparse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("quant.engine.cli")


def _run_all_cmd() -> None:
    from quant.engine.orchestrator import run_all
    run_all()


def _run_task_cmd(task_name: str) -> None:
    from quant.engine.orchestrator import run_task
    run_task(task_name)


def _run_pipeline_cmd(tasks: list[str]) -> None:
    from quant.engine.orchestrator import run_pipeline
    run_pipeline(tasks or None)


def main() -> None:
    parser = argparse.ArgumentParser(description="Quant engine CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_all = subparsers.add_parser("all", help="Run full pipeline")
    p_all.set_defaults(func=lambda args: _run_all_cmd())

    p_task = subparsers.add_parser("task", help="Run a single task")
    p_task.add_argument("name", help="Task name")
    p_task.set_defaults(func=lambda args: _run_task_cmd(args.name))

    p_pipe = subparsers.add_parser("pipeline", help="Run subset of tasks")
    p_pipe.add_argument("tasks", nargs="*", help="Task names")
    p_pipe.set_defaults(func=lambda args: _run_pipeline_cmd(args.tasks))

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()