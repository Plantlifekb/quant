from quant.engine.tasks import ingestion, prices, returns, dashboard


def inspect():
    return {
        "ingestion": ingestion.run.__name__,
        "prices": prices.run.__name__,
        "returns": returns.run.__name__,
        "dashboard": dashboard.run.__name__,
    }