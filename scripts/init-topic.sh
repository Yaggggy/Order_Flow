#!/bin/sh
set -eu
if ! rpk topic describe market.ticks --brokers redpanda:9092 >/dev/null 2>&1; then
  rpk topic create market.ticks --brokers redpanda:9092 --partitions 3 --replicas 1 \
    --topic-config retention.ms=259200000 --topic-config cleanup.policy=delete
fi
rpk topic describe market.ticks --brokers redpanda:9092

