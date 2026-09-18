CREATE TABLE IF NOT EXISTS order_flow.trades_kafka
(
    event_id         String,
    trade_id         String,
    source           String,
    symbol           String,
    timestamp_ms     Int64,
    price            Decimal(18, 8),
    quantity         Decimal(18, 8),
    is_buyer_maker   Bool
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'market.ticks',
    kafka_group_name = 'order-flow-clickhouse-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 3,
    kafka_max_block_size = 65536,
    kafka_flush_interval_ms = 500,
    kafka_poll_timeout_ms = 500,
    kafka_skip_broken_messages = 0,
    kafka_handle_error_mode = 'default',
    input_format_json_read_numbers_as_strings = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS order_flow.mv_trades_kafka_to_raw
TO order_flow.trades_raw
AS
SELECT
    event_id,
    trade_id,
    source,
    symbol,
    fromUnixTimestamp64Milli(timestamp_ms) AS timestamp,
    price,
    quantity,
    is_buyer_maker,
    if(is_buyer_maker, CAST(0 AS Decimal(18, 8)), quantity) AS ask_volume,
    if(is_buyer_maker, quantity, CAST(0 AS Decimal(18, 8))) AS bid_volume,
    quantity AS total_volume
FROM order_flow.trades_kafka;
