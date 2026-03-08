import subprocess
import sys

packages = [
    "numpy",
    "pandas",
    "scipy",
    "scikit-learn",
    "statsmodels",
    "matplotlib",
    "seaborn",
    "requests",
    "psycopg[binary]",
    "flask",
    "fastapi",
    "uvicorn[standard]",
    "pydantic",
    "python-dotenv",
    "ta-lib",
]

def install(pkg):
    print(f"\n=== Installing {pkg} ===")
    subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])

for pkg in packages:
    install(pkg)

print("\nAll packages installed.")