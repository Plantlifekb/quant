# logging_quant_v1.py
# Minimal, stable logging facade for the Quant pipelines.
# Exports: get_logger(name) and module-level `log` object for backward compatibility.

import logging
import sys
from typing import Any

_DEFAULT_LEVEL = logging.INFO

def _configure_logger(logger: logging.Logger) -> None:
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        fmt = logging.Formatter("[%(asctime)s UTC] [%(name)s] %(message)s")
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    logger.setLevel(_DEFAULT_LEVEL)

def get_logger(name: str = "quant_v1") -> logging.Logger:
    logger = logging.getLogger(name)
    _configure_logger(logger)
    return logger

class _ModuleLogProxy:
    def __init__(self, default_name: str = "quant_v1"):
        self._name = default_name
    def _logger(self) -> logging.Logger:
        return get_logger(self._name)
    def info(self, *args: Any, **kwargs: Any) -> None:
        self._logger().info(*args, **kwargs)
    def warning(self, *args: Any, **kwargs: Any) -> None:
        self._logger().warning(*args, **kwargs)
    def error(self, *args: Any, **kwargs: Any) -> None:
        self._logger().error(*args, **kwargs)
    def debug(self, *args: Any, **kwargs: Any) -> None:
        self._logger().debug(*args, **kwargs)
    def exception(self, *args: Any, **kwargs: Any) -> None:
        self._logger().exception(*args, **kwargs)

log = _ModuleLogProxy()
