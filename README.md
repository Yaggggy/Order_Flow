# Order Flow

A real-time financial market-data project that ingests trade events, stores them in ClickHouse, aggregates them into minute footprint candles, and exposes them through a FastAPI API and live WebSocket stream.

This project was built to demonstrate a full pipeline for executed-trade footprint analytics, with live Binance support and synthetic fallback for local testing.

## Overview

The system collects executed market data, normalizes it into a consistent schema, publishes it to Kafka/Redpanda, stores it in ClickHouse, and then exposes the aggregated output as:

- REST API responses
- WebSocket snapshots
- Redis-published live updates

It is focused on trade footprint data, including:

- price levels
- ask volume and bid volume per level
- total volume
- signed delta
- point-of-control (POC)

This is not a traditional order-book depth system. It is designed around executed-trade footprints.

## Architecture

```text
Binance / synthetic trade stream
        |
        v
Ingestion service
        |
        v
Redpanda (Kafka-compatible broker)
        |
        v
ClickHouse raw storage + materialized aggregation
        |
        +--> FastAPI REST API
        |
        +--> Broadcaster service
                      |
                      v
                   Redis Pub/Sub
                      |
                      v
              WebSocket clients
```

## Tech Stack

- Python 3.12
- FastAPI
- Redis
- Redpanda / Kafka
- ClickHouse
- WebSockets
- Docker Compose
- Prometheus metrics
- pytest and Ruff
- Decimal-based precision for financial values

## Main Features

- live Binance trade ingestion
- synthetic trade generation for local/offline testing
- Kafka-based event pipeline
- ClickHouse raw and aggregate tables
- minute-level footprint analytics
- authenticated REST API
- live WebSocket snapshot delivery
- Dockerized local deployment
- end-to-end and unit test coverage

## Project Structure

```text
order_flow/
├── .env.example
├── .github/
├── compose.yaml
├── Dockerfile
├── Makefile
├── OPERATIONS.md
├── PROJECT_GUIDE.md
├── PROJECT_OVERVIEW.md
├── README.md
├── pyproject.toml
├── requirements.txt
├── config/
│   └── clickhouse/
├── orderflow/
│   ├── __init__.py
│   ├── analytics.py
│   ├── config.py
│   └── runtime.py
├── scripts/
│   ├── init-clickhouse.sh
│   ├── init-topic.sh
│   ├── load_test.py
│   ├── setup_env.py
│   └── ws_client.py
├── services/
│   ├── api/
│   ├── broadcaster/
│   └── ingestion/
├── sql/
│   ├── 001_schema.sql
│   └── 002_kafka.sql
├── tests/
│   ├── integration/
│   └── unit/
└── VALIDATION.md
```

## Quick Start

### 1. Generate environment variables

```bash
python3 scripts/setup_env.py
```

### 2. Start the stack

```bash
docker compose up -d --build
```

### 3. Check the API

Open:

```text
http://localhost:8000/docs
```

Use the generated API key from the `.env` file and authorize in Swagger UI.

### 4. View logs

```bash
docker compose logs -f --tail=100 ingestion broadcaster api
```

## Local Development

Run tests:

```bash
pytest -q
```

Run integration tests:

```bash
docker compose --profile test run --rm tests
```

Run the load test:

```bash
docker compose --profile test run --rm tests \
  python scripts/load_test.py --rate 1500 --seconds 10
```

## Environment Variables

Copy from `.env.example` and configure the real values in `.env`:

- `CLICKHOUSE_ADMIN_PASSWORD`
- `CLICKHOUSE_PASSWORD`
- `REDIS_PASSWORD`
- `API_KEY`
- `INGESTION_MODE`
- `SYMBOLS`
- `SYNTHETIC_TICKS_PER_SECOND`
- `BROADCAST_LOOKBACK_MINUTES`
- `MAX_PRICE_LEVELS`
- `ALLOWED_ORIGINS`

## API Example

The project exposes a footprint endpoint like:

```http
GET /api/v1/footprint/BTCUSDT?start=2026-09-09T06:00:00Z&end=2026-09-09T07:00:00Z
X-API-Key: <your-api-key>
```

It returns minute-level footprint candles with values such as:

- `price`
- `ask_volume`
- `bid_volume`
- `total_volume`
- `delta`
- `poc_price`
- `is_closed`

## WebSocket Streaming

The project also exposes a live WebSocket endpoint:

```text
ws://localhost:8000/ws/orderbook/BTCUSDT
```

Clients must authenticate with an API key message like:

```json
{"type":"auth","api_key":"YOUR_API_KEY"}
```

## Important Notes

- The project is designed for a local/single-node deployment, not a production-grade financial exchange system.
- It uses `Decimal` instead of floating point to preserve exact financial precision.
- Synthetic and live datasets should be kept separate.
- The project is meant for learning, prototyping, and local analytics pipelines.

## Repository Status

This repository contains a complete working example of:

- data ingestion
- Kafka streaming
- ClickHouse analytics
- REST + WebSocket APIs
- Redis-based live broadcasting
- Docker-based deployment
- automated test structure

## License

This project is for personal and educational use.

## Future Improvements

- better production deployment hardening
- Grafana dashboards
- metric alerting
- persistent multi-node orchestration
- more advanced market microstructure analytics
- frontend dashboard for market footprint visualization

---
