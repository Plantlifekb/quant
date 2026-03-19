.DEFAULT_GOAL := help

.PHONY: \
	build up down logs ps run-all run-task \
	dashboard.build dashboard.up dashboard.logs dashboard.shell \
	orchestrator.build orchestrator.up orchestrator.logs orchestrator.shell \
	db.up db.logs db.shell \
	help

help:
	@echo "Available targets:"
	@echo "  build                Build all services"
	@echo "  up                   Start all services in detached mode"
	@echo "  down                 Stop all services"
	@echo "  logs                 Follow logs for all services"
	@echo "  ps                   Show service status"
	@echo "  run-all              Run full engine cycle"
	@echo "  run-task task=<t>    Run a specific engine task"
	@echo "  dashboard.build      Build dashboard service"
	@echo "  dashboard.up         Start dashboard service"
	@echo "  dashboard.logs       Follow dashboard logs"
	@echo "  dashboard.shell      Shell into dashboard container"
	@echo "  orchestrator.build   Build orchestrator service"
	@echo "  orchestrator.up      Start orchestrator service"
	@echo "  orchestrator.logs    Follow orchestrator logs"
	@echo "  orchestrator.shell   Shell into orchestrator container"
	@echo "  db.up                Start database service"
	@echo "  db.logs              Follow database logs"
	@echo "  db.shell             Shell into database container"


PROJECT_NAME := quant

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

run-all:
	docker compose run --rm engine python -m quant.engine.cli run-all

run-task:
	@if [ -z "$(task)" ]; then echo "Usage: make run-task task=<name>"; exit 1; fi
	docker compose run --rm engine python -m quant.engine.cli run-task $(task)


dashboard.build:
	docker compose build dashboard

dashboard.up:
	docker compose up dashboard

dashboard.logs:
	docker compose logs -f dashboard

dashboard.shell:
	docker compose exec dashboard sh


orchestrator.build:
	docker compose build orchestrator

orchestrator.up:
	docker compose up orchestrator

orchestrator.logs:
	docker compose logs -f orchestrator

orchestrator.shell:
	docker compose exec orchestrator sh


db.up:
	docker compose up db

db.logs:
	docker compose logs -f db

db.shell:
	docker compose exec db sh