IMAGE_NAME := quant
TAG := latest

build:
	docker build -t $(IMAGE_NAME):$(TAG) .

run-dashboard:
	docker run --rm -p 8050:8050 $(IMAGE_NAME):$(TAG)

run-engine:
	docker run --rm $(IMAGE_NAME):$(TAG) python -m quant.engine.main

compose-up:
	docker compose up --build

compose-down:
	docker compose down