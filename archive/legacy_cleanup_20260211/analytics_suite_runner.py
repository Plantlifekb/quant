import pandas as pd

from quant.analytics.return_engine import return_engine
# from quant.analytics.risk_engine import risk_engine
# from quant.analytics.portfolio_engine_quant_v2 import portfolio_engine
# from quant.analytics.factor_engine import factor_engine


def run_analytics_suite(prices: pd.DataFrame) -> dict:
    """
    Orchestrate the analytics engines in a governed sequence.

    Inputs
    -------
    prices : raw prices DataFrame

    Outputs
    -------
    dict with:
        - prices_enriched : prices with return suite
        # - risk_enriched
        # - portfolio_enriched
        # - factor_enriched
    """

    # 1. return engine (canonical first step)
    prices_enriched = return_engine(prices)

    # 2. risk engine (placeholder for now)
    # risk_enriched = risk_engine(prices_enriched)

    # 3. factor engine (placeholder)
    # factor_enriched = factor_engine(prices_enriched)

    # 4. portfolio engine (placeholder)
    # portfolio_enriched = portfolio_engine(prices_enriched)

    return {
        "prices_enriched": prices_enriched,
        # "risk_enriched": risk_enriched,
        # "factor_enriched": factor_enriched,
        # "portfolio_enriched": portfolio_enriched,
    }