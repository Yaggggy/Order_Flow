.PHONY: setup up down logs test integration load lint
setup:
	python3 scripts/setup_env.py
up: setup
	docker compose up -d --build

down:
	docker compose down
logs:
	docker compose logs -f --tail=100 ingestion broadcaster api
test:
	python -m pytest -q
lint:
	ruff check .
integration:
	docker compose --profile test run --rm tests
load:
	docker compose --profile test run --rm tests python scripts/load_test.py --rate 1500 --seconds 10
