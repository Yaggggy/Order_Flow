#!/bin/sh
# Bootstrap ClickHouse schema and the least-privileged analytics user.
# This script is run by an official ClickHouse image with /sql and /scripts
# mounted into it.  Compose healthchecks provide readiness before this job starts.
set -eu

CH_HOST=${CLICKHOUSE_HOST:-clickhouse}
CH_NATIVE_PORT=${CLICKHOUSE_NATIVE_PORT:-9000}
APP_USER=${CLICKHOUSE_USER:-orderflow}
APP_PASSWORD=${CLICKHOUSE_PASSWORD:-}
ADMIN_PASSWORD=${CLICKHOUSE_ADMIN_PASSWORD:-}

# Values are interpolated into the CREATE USER statement below.  Keep the
# accepted alphabet deliberately narrow so neither SQL nor shell metacharacters
# can enter from environment variables.  Passwords of 20+ characters are
# recommended by the deployment contract, but are not enforced for local dev.
validate_credential() {
    name=$1
    value=$2
    if [ -z "$value" ]; then
        echo "$name must be set and non-empty" >&2
        exit 2
    fi
    case "$value" in
        *[!ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-]*)
            echo "$name may contain only ASCII letters, digits, underscore, or hyphen" >&2
            exit 2
            ;;
    esac
}

validate_credential CLICKHOUSE_USER "$APP_USER"
validate_credential CLICKHOUSE_PASSWORD "$APP_PASSWORD"
validate_credential CLICKHOUSE_ADMIN_PASSWORD "$ADMIN_PASSWORD"

if [ ! -r /sql/001_schema.sql ] || [ ! -r /sql/002_kafka.sql ]; then
    echo "expected /sql/001_schema.sql and /sql/002_kafka.sql" >&2
    exit 2
fi

ch_client() {
    clickhouse-client \
        --host "$CH_HOST" \
        --port "$CH_NATIVE_PORT" \
        --user default \
        --password "$ADMIN_PASSWORD" \
        "$@"
}

# Downstream schema/view is in 001 and Kafka source/view is in 002; execute in
# that order so the materialized-view dependency is always present first.
ch_client --multiquery < /sql/001_schema.sql
ch_client --multiquery < /sql/002_kafka.sql
ch_client --multiquery <<EOF
CREATE USER IF NOT EXISTS \`$APP_USER\` IDENTIFIED WITH sha256_password BY '$APP_PASSWORD';
GRANT SELECT ON order_flow.footprint_bars_1m TO \`$APP_USER\`;
EOF
ch_client --query 'SELECT 1 FORMAT TSVRaw' >/dev/null
printf '%s\n' "ClickHouse schema and analytics user are ready"
