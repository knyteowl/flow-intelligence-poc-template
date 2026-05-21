#!/usr/bin/env bash
set -euo pipefail

# Native, no-container installer for the flow-intelligence POC.
# Target: Ubuntu/Debian-style internal POC server.
# Installs: ClickHouse, Kafka, goflow2, Grafana, Python scripts, systemd services, cron.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEPLOY_DIR="$ROOT_DIR/deploy/native-server"
ENV_FILE="${ENV_FILE:-$DEPLOY_DIR/.env}"
FLOW_HOME="${FLOW_HOME:-/opt/flow-poc}"
KAFKA_VERSION="${KAFKA_VERSION:-3.9.0}"
KAFKA_SCALA_VERSION="${KAFKA_SCALA_VERSION:-2.13}"
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
GOFLOW2_PORT="${GOFLOW2_PORT:-2056}"
GRAFANA_PORT="${GRAFANA_PORT:-3001}"
GRAFANA_BIND_ADDR="${GRAFANA_BIND_ADDR:-127.0.0.1}"
CLICKHOUSE_NATIVE_HOST="${CLICKHOUSE_NATIVE_HOST:-127.0.0.1}"
CLICKHOUSE_NATIVE_PORT="${CLICKHOUSE_NATIVE_PORT:-9000}"
KAFKA_BROKER="${KAFKA_BROKER:-127.0.0.1:9092}"
GOFLOW2_URL="${GOFLOW2_URL:-}"

log() { printf '\n==> %s\n' "$*"; }
warn() { printf '\nWARNING: %s\n' "$*" >&2; }
fail() { printf '\nERROR: %s\n' "$*" >&2; exit 1; }
need_root() { [ "$(id -u)" -eq 0 ] || fail "run as root, e.g. sudo $0"; }
need_cmd() { command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"; }
rand_secret() { openssl rand -base64 24 | tr -d '\n'; }

need_root
need_cmd bash
need_cmd openssl

if [ -r /etc/os-release ]; then
  # shellcheck disable=SC1091
  . /etc/os-release
else
  fail "/etc/os-release not found"
fi

case "${ID:-}" in
  ubuntu|debian) ;;
  *) warn "This installer is tested for Ubuntu/Debian. Detected ID=${ID:-unknown}. Continuing best-effort." ;;
esac

export DEBIAN_FRONTEND=noninteractive

log "Creating native deployment env file if needed"
install -d -m 755 "$DEPLOY_DIR"
if [ ! -f "$ENV_FILE" ]; then
  cp "$DEPLOY_DIR/.env.example" "$ENV_FILE"
  chmod 600 "$ENV_FILE"
  grafana_pw="$(rand_secret)"
  ch_pw="$(rand_secret)"
  sed -i "s/^GRAFANA_ADMIN_PASSWORD=.*/GRAFANA_ADMIN_PASSWORD=$grafana_pw/" "$ENV_FILE"
  sed -i "s/^CLICKHOUSE_GRAFANA_PASSWORD=.*/CLICKHOUSE_GRAFANA_PASSWORD=$ch_pw/" "$ENV_FILE"
fi
# shellcheck disable=SC1090
set -a; . "$ENV_FILE"; set +a

: "${GRAFANA_ADMIN_PASSWORD:?set in $ENV_FILE}"
: "${CLICKHOUSE_GRAFANA_PASSWORD:?set in $ENV_FILE}"

log "Installing base packages"
apt-get update
apt-get install -y ca-certificates curl gnupg lsb-release apt-transport-https jq openjdk-21-jre-headless python3 python3-venv python3-pip cron

log "Installing ClickHouse packages"
install -d -m 0755 /etc/apt/keyrings
if [ ! -f /etc/apt/keyrings/clickhouse.asc ]; then
  curl -fsSL https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key | gpg --dearmor -o /etc/apt/keyrings/clickhouse.gpg
fi
cat >/etc/apt/sources.list.d/clickhouse.list <<'EOF'
deb [signed-by=/etc/apt/keyrings/clickhouse.gpg] https://packages.clickhouse.com/deb stable main
EOF
apt-get update
apt-get install -y clickhouse-server clickhouse-client
systemctl enable --now clickhouse-server

log "Installing Kafka ${KAFKA_VERSION} under ${KAFKA_HOME}"
if [ ! -d "$KAFKA_HOME" ]; then
  tmpdir="$(mktemp -d)"
  trap 'rm -rf "$tmpdir"' EXIT
  kafka_tgz="kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}.tgz"
  kafka_url="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${kafka_tgz}"
  curl -fsSL "$kafka_url" -o "$tmpdir/$kafka_tgz"
  tar -xzf "$tmpdir/$kafka_tgz" -C /opt
  mv "/opt/kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}" "$KAFKA_HOME"
  useradd --system --home "$KAFKA_HOME" --shell /usr/sbin/nologin kafka 2>/dev/null || true
  chown -R kafka:kafka "$KAFKA_HOME"
fi
install -d -o kafka -g kafka -m 755 /var/lib/kafka /var/log/kafka
cat > /etc/systemd/system/flow-kafka.service <<EOF
[Unit]
Description=Flow POC Kafka broker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=kafka
Group=kafka
Environment=KAFKA_CLUSTER_ID=flow-poc-native-cluster
ExecStartPre=/bin/bash -lc 'if [ ! -f /var/lib/kafka/meta.properties ]; then ${KAFKA_HOME}/bin/kafka-storage.sh format -t \$(${KAFKA_HOME}/bin/kafka-storage.sh random-uuid) -c ${KAFKA_HOME}/config/kraft/server.properties; fi'
ExecStart=${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/kraft/server.properties
ExecStop=${KAFKA_HOME}/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=10
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
EOF
# Keep Kafka simple/local for MVP. Externally, goflow2 and ClickHouse run on same server.
sed -i "s|^log.dirs=.*|log.dirs=/var/lib/kafka|" "$KAFKA_HOME/config/kraft/server.properties"
sed -i "s|^listeners=.*|listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093|" "$KAFKA_HOME/config/kraft/server.properties"
sed -i "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://127.0.0.1:9092|" "$KAFKA_HOME/config/kraft/server.properties" || echo "advertised.listeners=PLAINTEXT://127.0.0.1:9092" >> "$KAFKA_HOME/config/kraft/server.properties"
systemctl daemon-reload
systemctl enable --now flow-kafka

log "Installing goflow2"
install -d -m 755 /usr/local/bin
if ! command -v goflow2 >/dev/null 2>&1; then
  tmpdir="$(mktemp -d)"
  if [ -n "$GOFLOW2_URL" ]; then
    curl -fsSL "$GOFLOW2_URL" -o "$tmpdir/goflow2_asset"
  else
    asset_url="$(curl -fsSL https://api.github.com/repos/netsampler/goflow2/releases/latest | jq -r '.assets[] | select((.name|test("linux.*amd64|amd64.*linux"; "i")) and (.name|test("tar.gz|tgz|zip|gz$"; "i"))) | .browser_download_url' | head -1)"
    [ -n "$asset_url" ] && [ "$asset_url" != "null" ] || fail "could not auto-detect goflow2 Linux amd64 release asset; set GOFLOW2_URL in $ENV_FILE"
    curl -fsSL "$asset_url" -o "$tmpdir/goflow2_asset"
  fi
  file "$tmpdir/goflow2_asset" || true
  if tar -tzf "$tmpdir/goflow2_asset" >/dev/null 2>&1; then
    tar -xzf "$tmpdir/goflow2_asset" -C "$tmpdir"
    found="$(find "$tmpdir" -type f -name 'goflow2' -perm /111 | head -1)"
  else
    found="$tmpdir/goflow2_asset"
  fi
  [ -n "${found:-}" ] || fail "goflow2 binary not found in release asset"
  install -m 755 "$found" /usr/local/bin/goflow2
fi
cat > /etc/systemd/system/flow-goflow2.service <<EOF
[Unit]
Description=Flow POC goflow2 collector
After=network-online.target flow-kafka.service
Wants=network-online.target flow-kafka.service

[Service]
Type=simple
ExecStart=/usr/local/bin/goflow2 -listen=netflow://:${GOFLOW2_PORT} -format=json -transport=kafka -transport.kafka.brokers=${KAFKA_BROKER} -transport.kafka.topic=flows.raw
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now flow-goflow2

log "Installing Grafana OSS"
if [ ! -f /etc/apt/keyrings/grafana.gpg ]; then
  curl -fsSL https://apt.grafana.com/gpg.key | gpg --dearmor -o /etc/apt/keyrings/grafana.gpg
fi
cat >/etc/apt/sources.list.d/grafana.list <<'EOF'
deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main
EOF
apt-get update
apt-get install -y grafana
install -d -m 755 /etc/grafana/provisioning/datasources /etc/grafana/provisioning/dashboards /var/lib/grafana/dashboards
cp "$ROOT_DIR"/grafana/dashboards/*.json /var/lib/grafana/dashboards/
cat >/etc/grafana/provisioning/dashboards/flow-poc.yml <<'EOF'
apiVersion: 1

providers:
  - name: Flow POC
    orgId: 1
    folder: Network Flow POC
    type: file
    disableDeletion: false
    editable: true
    options:
      path: /var/lib/grafana/dashboards
EOF
cat >/etc/grafana/provisioning/datasources/clickhouse-flow-poc.yml <<EOF
apiVersion: 1

datasources:
  - name: ClickHouse Flow POC
    type: grafana-clickhouse-datasource
    access: proxy
    uid: clickhouse-flow-poc
    isDefault: true
    editable: true
    jsonData:
      host: ${CLICKHOUSE_NATIVE_HOST}
      port: ${CLICKHOUSE_NATIVE_PORT}
      protocol: native
      defaultDatabase: flow_poc
      username: grafana
      tlsSkipVerify: false
    secureJsonData:
      password: ${CLICKHOUSE_GRAFANA_PASSWORD}
EOF
/usr/share/grafana/bin/grafana cli --pluginsDir /var/lib/grafana/plugins plugins install grafana-clickhouse-datasource || true
sed -i "s/^;http_addr =.*/http_addr = ${GRAFANA_BIND_ADDR}/" /etc/grafana/grafana.ini
sed -i "s/^;http_port =.*/http_port = ${GRAFANA_PORT}/" /etc/grafana/grafana.ini
sed -i "s/^;admin_user =.*/admin_user = ${GRAFANA_ADMIN_USER:-admin}/" /etc/grafana/grafana.ini
sed -i "s/^;admin_password =.*/admin_password = ${GRAFANA_ADMIN_PASSWORD}/" /etc/grafana/grafana.ini
systemctl enable --now grafana-server
systemctl restart grafana-server

log "Applying ClickHouse schema"
tmp_schema="$(mktemp)"
sed "s/kafka:9092/${KAFKA_BROKER//\//\/}/g" "$ROOT_DIR/sql/core_schema.sql" > "$tmp_schema"
clickhouse-client --multiquery < "$tmp_schema"
clickhouse-client --multiquery < "$ROOT_DIR/sql/external_ip_enrichment.sql"
clickhouse-client --multiquery < "$ROOT_DIR/sql/flow_rollups.sql"
clickhouse-client --multiquery < "$ROOT_DIR/sql/flow_baseline_views.sql"
clickhouse-client --query "CREATE USER IF NOT EXISTS grafana IDENTIFIED BY '${CLICKHOUSE_GRAFANA_PASSWORD}'"
clickhouse-client --query "GRANT SELECT ON flow_poc.* TO grafana"
rm -f "$tmp_schema"

log "Installing Python scripts"
install -d -m 700 "$FLOW_HOME" "$FLOW_HOME/secrets"
install -m 700 "$ROOT_DIR/scripts/"*.py "$FLOW_HOME/"
if [ ! -f "$FLOW_HOME/secrets/unifi.env" ]; then
  install -m 600 "$ROOT_DIR/config/unifi.env.example" "$FLOW_HOME/secrets/unifi.env"
fi

log "Installing cron jobs"
if ! crontab -l >/tmp/flow_poc_cron 2>/dev/null; then : > /tmp/flow_poc_cron; fi
grep -v '# flow-intelligence-poc managed' /tmp/flow_poc_cron > /tmp/flow_poc_cron.new || true
cat >> /tmp/flow_poc_cron.new <<'CRON'
*/5 * * * * /opt/flow-poc/refresh_flow_rollups.py >> /opt/flow-poc/refresh_flow_rollups.log 2>&1 # flow-intelligence-poc managed
7-59/15 * * * * /opt/flow-poc/sync_external_ip_enrichment.py --hours 24 --limit 3000 >> /opt/flow-poc/sync_external_ip_enrichment.log 2>&1 # flow-intelligence-poc managed
CRON
crontab /tmp/flow_poc_cron.new
rm -f /tmp/flow_poc_cron /tmp/flow_poc_cron.new
systemctl enable --now cron || true

log "Running smoke checks"
systemctl --no-pager --full status clickhouse-server flow-kafka flow-goflow2 grafana-server >/dev/null || true
clickhouse-client --query 'SELECT 1' >/dev/null
curl -fsS "http://${GRAFANA_BIND_ADDR}:${GRAFANA_PORT}/api/health" >/dev/null || warn "Grafana health check failed; it may still be starting"

echo
echo "Native deployment complete."
echo "NetFlow/IPFIX UDP listener: ${GOFLOW2_PORT}/udp"
echo "Grafana: http://${GRAFANA_BIND_ADDR}:${GRAFANA_PORT}"
echo "Grafana user: ${GRAFANA_ADMIN_USER:-admin}"
echo "Passwords/env: $ENV_FILE"
echo
echo "Verify flows after pointing an exporter at this host:"
echo "  clickhouse-client --query 'SELECT count(), max(received_at) FROM flow_poc.flows'"
echo "  /opt/flow-poc/analyze_flows.py --since 1h --format text"
