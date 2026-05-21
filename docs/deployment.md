# Deployment notes

These notes document the current POC deployment shape. Adjust paths/container names for other environments.

## Fast internal-server MVP path

For a command-line deployment using the same core tools as the VPS POC, use:

```bash
cd flow-intelligence-poc
sudo deploy/internal-poc/bootstrap.sh
```

That starts the non-AI stack:

```text
goflow2 -> Kafka -> ClickHouse -> Grafana
                  -> Python rollups/enrichment/reports
```

See `deploy/internal-poc/README.md` for prerequisites, exporter setup, and verification.

## Native no-container server path

If a container runtime is unavailable, use the direct server install path:

```bash
cd flow-intelligence-poc
sudo deploy/native-server/bootstrap.sh
```

This installs ClickHouse, Kafka, goflow2, Grafana, scripts, systemd services, and cron directly on the server. See `deploy/native-server/README.md`.

Tradeoff:

```text
No container runtime needed, but more host-specific package/service state.
```

## Target paths

```text
/opt/flow-poc/                  scripts, SQL, logs
/opt/flow-poc/secrets/          root-only env files
/opt/grafana/dashboards/        provisioned dashboard JSON
```

## Install/update scripts

```bash
sudo mkdir -p /opt/flow-poc /opt/flow-poc/secrets /opt/grafana/dashboards
sudo install -m 700 scripts/*.py /opt/flow-poc/
sudo install -m 644 sql/*.sql /opt/flow-poc/
sudo install -m 644 grafana/dashboards/*.json /opt/grafana/dashboards/
sudo install -m 600 config/unifi.env.example /opt/flow-poc/secrets/unifi.env
```

Edit `/opt/flow-poc/secrets/unifi.env` and replace `[REDACTED]` with real values.

## Create/update ClickHouse objects

```bash
docker exec -i clickhouse clickhouse-client --multiquery < /opt/flow-poc/flow_rollups.sql
docker exec -i clickhouse clickhouse-client --multiquery < /opt/flow-poc/flow_baseline_views.sql
docker exec -i clickhouse clickhouse-client --multiquery < /opt/flow-poc/external_ip_enrichment.sql
```

## Manual runs

```bash
/opt/flow-poc/sync_unifi_devices.py
/opt/flow-poc/sync_external_ip_enrichment.py --hours 24 --limit 2000
/opt/flow-poc/refresh_flow_rollups.py --skip-daily
/opt/flow-poc/analyze_flows.py --since 1h --format text
/opt/flow-poc/analyze_flow_baselines.py --format text
```

## Cron schedule used by the POC

```cron
*/5 * * * * /opt/flow-poc/sync_unifi_devices.py >> /opt/flow-poc/sync_unifi_devices.log 2>&1
2-59/5 * * * * /opt/flow-poc/refresh_flow_rollups.py --skip-daily >> /opt/flow-poc/refresh_flow_rollups.log 2>&1
17 0 * * * /opt/flow-poc/refresh_flow_rollups.py >> /opt/flow-poc/refresh_flow_rollups.log 2>&1
7-59/15 * * * * /opt/flow-poc/sync_external_ip_enrichment.py --hours 24 --limit 2000 >> /opt/flow-poc/sync_external_ip_enrichment.log 2>&1
```

## Grafana reload

The POC currently uses file-provisioned dashboard JSON. Restart Grafana after dashboard changes:

```bash
docker restart grafana
curl -sS http://127.0.0.1:3001/api/health
```

## Validation checks

```bash
docker exec clickhouse clickhouse-client --query "SHOW TABLES FROM flow_poc"
docker exec clickhouse clickhouse-client --query "SELECT max(received_at) FROM flow_poc.flows"
docker exec clickhouse clickhouse-client --query "SELECT count() FROM flow_poc.device_inventory_current"
docker exec clickhouse clickhouse-client --query "SELECT count(), max(bucket_start) FROM flow_poc.flow_rollup_5m"
docker exec clickhouse clickhouse-client --query "SELECT count() FROM flow_poc.external_ip_enrichment_current"
```
