<<<<<<< HEAD
﻿import logging
from logging import Logger

_log: Logger = logging.getLogger("quant")
if not _log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
    _log.addHandler(h)
    _log.setLevel(logging.INFO)

log = _log
=======
import logging
log = logging.getLogger("quant")
if not log.handlers:
    h = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    h.setFormatter(fmt)
    log.addHandler(h)
    log.setLevel(logging.INFO)
>>>>>>> 7d40ac5 (chore: add temporary logging shim to allow local ingestion runs; TODO replace with canonical logging implementation before merge)
