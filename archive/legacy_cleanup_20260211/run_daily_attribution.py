import subprocess
import datetime
import os
import shutil
from pathlib import Path

# ------------------------------------------------------------
# Daily Runner for Quant Attribution Suite v1.0 (Enhanced)
# ------------------------------------------------------------

BASE_DIR = Path(r"C:\Quant")

# Core scripts
ATTRIBUTION_SUITE = BASE_DIR / "scripts" / "analytics" / "attribution_suite_v1" / "run_attribution_suite_v1.py"
SIGNAL_ENGINE = BASE_DIR / "scripts" / "run_signal_engine_v1_2.py"

# Dashboard entry point
DASHBOARD_ENTRY = BASE_DIR / "scripts" / "dashboard" / "quant_dashboard_v2.py"

# Logs
LOG_DIR = BASE_DIR / "logs" / "attribution_suite"
os.makedirs(LOG_DIR, exist_ok=True)


def run_python_script(script_path: Path, log):
    """Run a Python script and stream output to log."""
    log.write(f"\n[{datetime.datetime.utcnow()}] Running: {script_path}\n")
    log.write("------------------------------------------------------------\n")

    process = subprocess.Popen(
        ["python", str(script_path)],
        stdout=log,
        stderr=log,
        shell=True
    )
    process.wait()

    log.write(f"\nReturn code: {process.returncode}\n")
    return process.returncode


def launch_dashboard():
    """Launch Streamlit dashboard in a non-blocking subprocess."""
    subprocess.Popen(
        ["python", "-m", "streamlit", "run", str(DASHBOARD_ENTRY)],
        cwd=str(DASHBOARD_ENTRY.parent)
    )


def enforce_retention(directory, days=30):
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=days)
    for filename in os.listdir(directory):
        if filename.endswith(".log") and filename != "latest.log":
            timestamp = filename.replace(".log", "")
            try:
                file_time = datetime.datetime.strptime(timestamp, "%Y-%m-%d_%H-%M-%S")
                if file_time < cutoff:
                    os.remove(os.path.join(directory, filename))
            except:
                continue


def main():
    # Timestamp
    run_id = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    log_path = LOG_DIR / f"{run_id}.log"
    latest_log = LOG_DIR / "latest.log"
    status_file = LOG_DIR / "latest_status.txt"

    # Open log
    with open(log_path, "w", encoding="utf-8") as log:
        log.write(f"[{run_id}] Starting daily Quant automation\n")
        log.write("============================================================\n")

        # ------------------------------------------------------------
        # 1. Run Signal Engine (updates signals)
        # ------------------------------------------------------------
        log.write("\n[STEP 1] Running Signal Engine\n")
        signal_status = run_python_script(SIGNAL_ENGINE, log)

        # ------------------------------------------------------------
        # 2. Run Attribution Suite
        # ------------------------------------------------------------
        log.write("\n[STEP 2] Running Attribution Suite\n")
        attribution_status = run_python_script(ATTRIBUTION_SUITE, log)

        # ------------------------------------------------------------
        # 3. Launch Dashboard
        # ------------------------------------------------------------
        log.write("\n[STEP 3] Launching Dashboard\n")
        launch_dashboard()
        log.write("Dashboard launched successfully.\n")

        # Final status
        final_status = "SUCCESS" if (signal_status == 0 and attribution_status == 0) else "FAILURE"
        log.write("\n============================================================\n")
        log.write(f"FINAL STATUS: {final_status}\n")

    # Update latest log
    shutil.copyfile(log_path, latest_log)

    # Write status file
    with open(status_file, "w", encoding="utf-8") as f:
        f.write(final_status + "\n")

    # Retention
    enforce_retention(LOG_DIR, days=30)


if __name__ == "__main__":
    main()