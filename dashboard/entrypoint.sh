#!/bin/sh
set -euo pipefail

# Optional: wait for initializer to finish
sleep 2

cd /app
exec python -m app
