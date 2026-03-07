import logging
from logging import Logger

_log: Logger = logging.getLogger("quant")
if not _log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
    _log.addHandler(h)
    _log.setLevel(logging.INFO)

log = _log
