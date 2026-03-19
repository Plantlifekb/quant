# Scripts Overview

This directory contains operational scripts for interacting with the quant platform.
Each script is atomic, single‑purpose, and aligned with the docker-compose services.

All scripts are intended to be run from the repository root.

## Service Shell Access

### Dashboard
scripts/dashboard.sh  
Opens an interactive shell inside the dashboard container.

### Orchestrator
scripts/orchestrator.sh  
Opens an interactive shell inside the orchestrator container.

### Database
scripts/db.sh  
Opens an interactive shell inside the Postgres container.


## Platform Lifecycle

### Start all services
scripts/up.sh  
Starts all services in detached mode.

### Stop all services
scripts/down.sh  
Stops and removes all services.

### Follow logs
scripts/logs.sh  
Streams logs from all services.


## Build and Rebuild

### Rebuild and restart everything
scripts/rebuild.sh  
Rebuilds all images and restarts the platform.