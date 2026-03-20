import logging
log = logging.getLogger("quant")
if not log.handlers:
    h = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    h.setFormatter(fmt)
    log.addHandler(h)
    log.setLevel(logging.INFO)
