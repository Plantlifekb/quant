from quant.engine.tasks import (
    ingestion as ingestion_task,
    prices as prices_task,
    returns as returns_task,
    signals as signals_task,
    strategies as strategies_task,
    dashboard as dashboard_task,
    fundamentals as fundamentals_task,
)


def get_task_registry():
    return {
        "ingestion": ingestion_task.run,
        "prices": prices_task.run,
        "returns": returns_task.run,
        "signals": signals_task.run,
        "strategies": strategies_task.run,
        "dashboard": dashboard_task.run,
        "fundamentals": fundamentals_task.run,
    }