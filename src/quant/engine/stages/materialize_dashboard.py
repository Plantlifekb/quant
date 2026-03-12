# quant/engine/stages/materialize_dashboard.py
from sqlalchemy import text
import logging

logger = logging.getLogger("quant.engine.materialize_dashboard")

SQL_CREATE_WEEKLY = """
CREATE MATERIALIZED VIEW IF NOT EXISTS public.strategy_weekly_realized AS
WITH weeks AS (
  SELECT generate_series((current_date - INTERVAL '5 years')::date, current_date::date, '1 week')::date AS week_start
),
entries AS (
  SELECT s.strategy_id, w.week_start, s.ticker, COALESCE(s.weight,1.0) AS weight
  FROM weeks w
  JOIN public.strategy_selections s ON s.selection_date = w.week_start
),
realized AS (
  SELECT e.strategy_id, e.week_start, e.ticker,
         p0.close AS entry_price, p1.close AS exit_price,
         CASE WHEN p0.close IS NULL OR p1.close IS NULL THEN NULL
              ELSE (p1.close / p0.close - 1.0) * 100.0 END AS realized_pct,
         e.weight
  FROM entries e
  LEFT JOIN public.prices p0 ON p0.ticker = e.ticker AND p0.date = e.week_start
  LEFT JOIN public.prices p1 ON p1.ticker = e.ticker AND p1.date = e.week_start + INTERVAL '7 days'
),
strategy_weekly AS (
  SELECT strategy_id, week_start,
         SUM(realized_pct * weight) / NULLIF(SUM(weight),0) AS weighted_return_pct
  FROM realized
  GROUP BY strategy_id, week_start
),
hindsight_weekly AS (
  SELECT 'HINDSIGHT'::text AS strategy_id, r.week_start,
         (SELECT AVG(realized_pct) FROM (
            SELECT realized_pct FROM realized r2
            WHERE r2.week_start = r.week_start
            ORDER BY realized_pct DESC
            LIMIT 10
         ) t) AS weighted_return_pct
  FROM (SELECT DISTINCT week_start FROM realized) r
)
SELECT strategy_id, week_start, ROUND(weighted_return_pct::numeric, 1) AS weekly_return_pct
FROM (
  SELECT * FROM strategy_weekly
  UNION ALL
  SELECT * FROM hindsight_weekly
) t;
"""

SQL_CREATE_ANNUAL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS public.strategy_annual_summary AS
WITH weekly AS (
  SELECT strategy_id, week_start, weekly_return_pct
  FROM public.strategy_weekly_realized
),
agg AS (
  SELECT EXTRACT(YEAR FROM week_start)::int AS year,
         strategy_id,
         ROUND(AVG(weekly_return_pct)::numeric, 1) AS perf_1w_pct,
         ROUND( ( (EXP(SUM(LN(1 + COALESCE(weekly_return_pct,0)/100.0))/ (52/12)) - 1) * 100 )::numeric, 1) AS perf_1m_pct,
         ROUND( ( (EXP(SUM(LN(1 + COALESCE(weekly_return_pct,0)/100.0))) - 1) * 100 )::numeric, 1) AS perf_1y_pct
  FROM weekly
  GROUP BY year, strategy_id
)
SELECT * FROM agg;
"""

SQL_CREATE_RGAP = """
CREATE MATERIALIZED VIEW IF NOT EXISTS public.strategy_rgap_annual AS
SELECT s.year, s.strategy_id,
       ROUND((h.perf_1y_pct - s.perf_1y_pct)::numeric, 1) AS r_gap_1y_pct
FROM public.strategy_annual_summary s
JOIN public.strategy_annual_summary h ON h.year = s.year AND h.strategy_id = 'HINDSIGHT'
WHERE s.strategy_id <> 'HINDSIGHT';
"""

SQL_REFRESH_WEEKLY = "REFRESH MATERIALIZED VIEW CONCURRENTLY public.strategy_weekly_realized;"
SQL_REFRESH_ANNUAL = "REFRESH MATERIALIZED VIEW CONCURRENTLY public.strategy_annual_summary;"
SQL_REFRESH_RGAP = "REFRESH MATERIALIZED VIEW CONCURRENTLY public.strategy_rgap_annual;"


def create_materialized_views(engine):
    logger.info("Creating materialized views (if not exists)")
    with engine.begin() as conn:
        conn.execute(text(SQL_CREATE_WEEKLY))
        conn.execute(text(SQL_CREATE_ANNUAL))
        conn.execute(text(SQL_CREATE_RGAP))
    logger.info("Materialized view creation complete")


def refresh_materialized_views(engine):
    logger.info("Refreshing materialized views (concurrently)")
    with engine.begin() as conn:
        conn.execute(text(SQL_REFRESH_WEEKLY))
        conn.execute(text(SQL_REFRESH_ANNUAL))
        conn.execute(text(SQL_REFRESH_RGAP))
    logger.info("Materialized view refresh complete")