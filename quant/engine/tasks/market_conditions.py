from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List

from sqlalchemy import text
from quant.engine.db import create_db_engine
import math


@dataclass
class DailyMetrics:
    as_of: object
    avg_ret: float
    vol: float
    breadth: float
    regime: str
    vol_regime: str


def _ensure_tables(conn):
    ddl = """
    CREATE TABLE IF NOT EXISTS market_metrics_daily (
        as_of DATE PRIMARY KEY,
        avg_ret NUMERIC(18,10),
        vol NUMERIC(18,10),
        breadth NUMERIC(18,10),
        regime TEXT,
        vol_regime TEXT,
        created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS market_regime_daily (
        as_of DATE PRIMARY KEY,
        regime TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS volatility_regime_daily (
        as_of DATE PRIMARY KEY,
        vol_regime TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS breadth_daily (
        as_of DATE PRIMARY KEY,
        breadth NUMERIC(18,10) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """
    conn.execute(text(ddl))


def _classify_regime(avg_ret: float, vol: float) -> str:
    if avg_ret is None:
        return "unknown"
    if avg_ret > 0.001:
        return "bull"
    if avg_ret < -0.001:
        return "bear"
    return "sideways"


def _classify_vol_regime(vol: float) -> str:
    if vol is None:
        return "unknown"
    if vol < 0.005:
        return "low"
    if vol > 0.02:
        return "high"
    return "normal"


def run():
    engine = create_db_engine()

    with engine.begin() as conn:
        _ensure_tables(conn)

        rows = conn.execute(
            text("""
                SELECT as_of, symbol, ret_simple
                FROM returns_daily
                ORDER BY as_of, symbol
            """)
        ).fetchall()

        if not rows:
            return

        by_date: Dict[object, List[float]] = defaultdict(list)
        for as_of, symbol, ret_simple in rows:
            if ret_simple is not None:
                by_date[as_of].append(float(ret_simple))

        metrics: List[DailyMetrics] = []

        for as_of, rets in sorted(by_date.items(), key=lambda x: x[0]):
            if not rets:
                avg_ret = None
                vol = None
                breadth = None
            else:
                avg_ret = sum(rets) / len(rets)
                mean = avg_ret
                var = sum((r - mean) ** 2 for r in rets) / len(rets)
                vol = math.sqrt(var)
                breadth = sum(1 for r in rets if r > 0) / len(rets)

            regime = _classify_regime(avg_ret, vol)
            vol_regime = _classify_vol_regime(vol)

            metrics.append(
                DailyMetrics(
                    as_of=as_of,
                    avg_ret=avg_ret,
                    vol=vol,
                    breadth=breadth,
                    regime=regime,
                    vol_regime=vol_regime,
                )
            )

        for m in metrics:
            conn.execute(
                text("""
                    INSERT INTO market_metrics_daily (as_of, avg_ret, vol, breadth, regime, vol_regime)
                    VALUES (:as_of, :avg_ret, :vol, :breadth, :regime, :vol_regime)
                    ON CONFLICT (as_of) DO UPDATE
                    SET avg_ret = EXCLUDED.avg_ret,
                        vol = EXCLUDED.vol,
                        breadth = EXCLUDED.breadth,
                        regime = EXCLUDED.regime,
                        vol_regime = EXCLUDED.vol_regime
                """),
                {
                    "as_of": m.as_of,
                    "avg_ret": m.avg_ret,
                    "vol": m.vol,
                    "breadth": m.breadth,
                    "regime": m.regime,
                    "vol_regime": m.vol_regime,
                },
            )

            conn.execute(
                text("""
                    INSERT INTO market_regime_daily (as_of, regime)
                    VALUES (:as_of, :regime)
                    ON CONFLICT (as_of) DO UPDATE
                    SET regime = EXCLUDED.regime
                """),
                {"as_of": m.as_of, "regime": m.regime},
            )

            conn.execute(
                text("""
                    INSERT INTO volatility_regime_daily (as_of, vol_regime)
                    VALUES (:as_of, :vol_regime)
                    ON CONFLICT (as_of) DO UPDATE
                    SET vol_regime = EXCLUDED.vol_regime
                """),
                {"as_of": m.as_of, "vol_regime": m.vol_regime},
            )

            conn.execute(
                text("""
                    INSERT INTO breadth_daily (as_of, breadth)
                    VALUES (:as_of, :breadth)
                    ON CONFLICT (as_of) DO UPDATE
                    SET breadth = EXCLUDED.breadth
                """),
                {"as_of": m.as_of, "breadth": m.breadth},
            )