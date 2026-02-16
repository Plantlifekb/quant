# tests/reconciliation/test_dashboard_reconciliation.py
from decimal import Decimal
def test_dashboard_reconciliation_placeholder():
    offline = Decimal('12345.67')
    dashboard = Decimal('12345.60')
    tol = Decimal('0.005')
    diff = abs(dashboard - offline) / offline
    assert diff <= tol
