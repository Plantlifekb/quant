"""
Quant engine package.

Deterministic, import‑safe, and aligned with the orchestrator/DAG architecture.
This module intentionally does NOT import the orchestrator at top level.
"""

__all__ = []
from .orchestrator import run_all, run_task

