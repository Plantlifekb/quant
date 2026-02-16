from dashboard_app import compute_attribution
import traceback

try:
    daily, top, sector = compute_attribution()
    print("DAILY rows:", len(daily))
    print("TOP rows:", len(top))
    print("DAILY columns:", list(daily.columns))
    print("TOP columns:", list(top.columns))
except Exception:
    traceback.print_exc()