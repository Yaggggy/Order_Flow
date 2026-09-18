-- Order-flow storage schema for ClickHouse 25.3 LTS.
-- This is an idempotent bootstrap, not a migration mechanism.  Existing tables are
-- not altered when their definitions differ from this contract.

CREATE DATABASE IF NOT EXISTS order_flow;

CREATE TABLE IF NOT EXISTS order_flow.trades_raw
(
    event_id         String,
    trade_id         String,
    source           LowCardinality(String),
    symbol           LowCardinality(String),
    timestamp        DateTime64(3, 'UTC'),
    price            Decimal(18, 8),
    quantity         Decimal(18, 8),
    is_buyer_maker   Bool,
    ask_volume       Decimal(18, 8),
    bid_volume       Decimal(18, 8),
    total_volume     Decimal(18, 8),
    ingested_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (symbol, toStartOfMinute(timestamp), price)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS order_flow.footprint_bars_1m
(
    symbol            LowCardinality(String),
    minute            DateTime('UTC'),
    price             Decimal(18, 8),
    ask_volume_state  AggregateFunction(sum, Decimal(18, 8)),
    bid_volume_state  AggregateFunction(sum, Decimal(18, 8)),
    total_volume_state AggregateFunction(sum, Decimal(18, 8))
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (symbol, minute, price)
TTL minute + INTERVAL 90 DAY;

-- Build downstream first: the Kafka-to-raw view in sql/002_kafka.sql can then
-- attach to trades_raw without ever preceding the aggregation target/view.
CREATE MATERIALIZED VIEW IF NOT EXISTS order_flow.mv_trades_raw_to_footprint
TO order_flow.footprint_bars_1m
AS
SELECT
    symbol,
    toStartOfMinute(timestamp) AS minute,
    price,
    sumState(ask_volume) AS ask_volume_state,
    sumState(bid_volume) AS bid_volume_state,
    sumState(total_volume) AS total_volume_state
FROM order_flow.trades_raw
GROUP BY symbol, minute, price;
