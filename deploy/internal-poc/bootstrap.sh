#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEPLOY_DIR="$ROOT_DIR/deploy/internal-poc"
ENV_FILE="$DEPLOY_DIR/.env"

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "ERROR: required command not found: $1" >&2; exit 1; }
}

rand_secret() {
  openssl rand -base64 24 | tr -d '\n'
}

need_cmd docker
need_cmd openssl

if ! docker compose version >/dev/null 2>&1; then
  echo "ERROR: docker compose plugin is required" >&2
  exit 1
fi

if [ ! -f "$ENV_FILE" ]; then
  cp "$DEPLOY_DIR/.env.example" "$ENV_FILE"
  chmod 600 "$ENV_FILE"
  grafana_pw="$(rand_secret)"
  ch_pw="$(rand_secret)"
  sed -i "s/^GRAFANA_ADMIN_PASSWORD=.*/GRAFANA_ADMIN_PASSWORD=$grafana_pw/" "$ENV_FILE"
  sed -i "s/^CLICKHOUSE_GRAFANA_PASSWORD=.*/CLICKHOUSE_GRAFANA_PASSWORD=$ch_pw/" "$ENV_FILE"
  echo "Created $ENV_FILE with generated local passwords."
fi

set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a

if [ -z "${GRAFANA_ADMIN_PASSWORD:-}" ] || [ "$GRAFANA_ADMIN_PASSWORD" = "change-me" ]; then
  echo "ERROR: set GRAFANA_ADMIN_PASSWORD in $ENV_FILE" >&2
  exit 1
fi
if [ -z "${CLICKHOUSE_GRAFANA_PASSWORD:-}" ] || [ "$CLICKHOUSE_GRAFANA_PASSWORD" = "change-me" ]; then
  echo "ERROR: set CLICKHOUSE_GRAFANA_PASSWORD in $ENV_FILE" >&2
  exit 1
fi

cd "$DEPLOY_DIR"

echo "Starting flow POC containers..."
docker compose up -d kafka clickhouse

echo "Waiting for ClickHouse..."
for _ in $(seq 1 60); do
  if docker exec flow-clickhouse clickhouse-client --query 'SELECT 1' >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
docker exec flow-clickhouse clickhouse-client --query 'SELECT 1' >/dev/null

TMP_SQL="$(mktemp)"
trap 'rm -f "$TMP_SQL"' EXIT
cat > "$TMP_SQL" <<SQL
CREATE USER IF NOT EXISTS grafana IDENTIFIED BY '${CLICKHOUSE_GRAFANA_PASSWORD}';
GRANT SELECT ON flow_poc.* TO grafana;
SQL

echo "Applying ClickHouse schema..."
docker exec -i flow-clickhouse clickhouse-client --multiquery < "$ROOT_DIR/sql/core_schema.sql"
docker exec -i flow-clickhouse clickhouse-client --multiquery < "$ROOT_DIR/sql/external_ip_enrichment.sql"
docker exec -i flow-clickhouse clickhouse-client --multiquery < "$ROOT_DIR/sql/dns_correlation.sql"
docker exec -i flow-clickhouse clickhouse-client --multiquery < "$ROOT_DIR/sql/flow_rollups.sql"
docker exec -i flow-clickhouse clickhouse-client --multiquery < "$ROOT_DIR/sql/flow_baseline_views.sql"
docker exec -i flow-clickhouse clickhouse-client --multiquery < "$TMP_SQL"

echo "Starting collector and Grafana..."
docker compose up -d

echo "Installing scripts under /opt/flow-poc..."
install -d -m 700 /opt/flow-poc /opt/flow-poc/secrets
install -m 700 "$ROOT_DIR/scripts/"*.py /opt/flow-poc/
if [ ! -f /opt/flow-poc/secrets/unifi.env ]; then
  install -m 600 "$ROOT_DIR/config/unifi.env.example" /opt/flow-poc/secrets/unifi.env
fi

if ! crontab -l >/tmp/flow_poc_cron 2>/dev/null; then
  : > /tmp/flow_poc_cron
fi
# Remove prior managed entries, then add current schedule.
grep -v '# flow-intelligence-poc managed' /tmp/flow_poc_cron > /tmp/flow_poc_cron.new || true
cat >> /tmp/flow_poc_cron.new <<'CRON'
*/5 * * * * /opt/flow-poc/refresh_flow_rollups.py >> /opt/flow-poc/refresh_flow_rollups.log 2>&1 # flow-intelligence-poc managed
7-59/15 * * * * /opt/flow-poc/sync_external_ip_enrichment.py --hours 24 --limit 3000 >> /opt/flow-poc/sync_external_ip_enrichment.log 2>&1 # flow-intelligence-poc managed
CRON
crontab /tmp/flow_poc_cron.new
rm -f /tmp/flow_poc_cron /tmp/flow_poc_cron.new

echo
echo "Deployment complete."
echo "NetFlow/IPFIX UDP listener: ${NETFLOW_LISTEN_PORT:-2055}/udp"
echo "Grafana: http://${GRAFANA_BIND_ADDR:-127.0.0.1}:${GRAFANA_PORT:-3001}"
echo "Grafana user: ${GRAFANA_ADMIN_USER:-admin}"
echo "Grafana password is in: $ENV_FILE"
echo
echo "Next steps:"
echo "1. Point one test exporter at this server UDP/${NETFLOW_LISTEN_PORT:-2055}."
echo "2. Optional: edit /opt/flow-poc/secrets/unifi.env and run /opt/flow-poc/sync_unifi_devices.py."
echo "3. Verify: docker exec flow-clickhouse clickhouse-client --query 'SELECT count() FROM flow_poc.flows'"
