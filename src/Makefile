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