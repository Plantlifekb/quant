# quant/run.py

"""
Canonical entrypoint for the quant platform.
Deterministic, narratable, and aligned with the orchestrator.
"""

from quant.engine import run_all, run_task


def main():
    """
    Runs the full DAG deterministically.
    """
    run_all()


if __name__ == "__main__":
    main()