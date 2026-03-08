#!/usr/bin/env python3
# quant.common/config.py
import logging
import os

def configure_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=getattr(logging, level, logging.INFO))
