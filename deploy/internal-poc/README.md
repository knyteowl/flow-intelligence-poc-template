# Internal POC command-line deployment

This directory turns the non-AI flow visibility stack into a repeatable internal-server deployment using the same core tools as the VPS POC:

```text
goflow2 -> Kafka -> ClickHouse -> Grafana
                  -> Python rollups/enrichment/reports
```

## Prerequisites

Target server:

```text
Linux server or VM
Docker Engine
Docker Compose plugin
Outbound DNS/WHOIS access for external IP enrichment, if allowed
UDP/2055 reachable from selected NetFlow/IPFIX exporters
```

## One-command bootstrap

From a clone of this repo on the internal server:

```bash
cd flow-intelligence-poc
sudo deploy/internal-poc/bootstrap.sh
```

The bootstrap will:

```text
create deploy/internal-poc/.env with generated local passwords
start Kafka, ClickHouse, goflow2, and Grafana
apply ClickHouse schema/views/rollups
provision the ClickHouse datasource and Grafana dashboards
install report/enrichment scripts under /opt/flow-poc
install cron entries for rollups and external IP enrichment
```

## Access

Default Grafana binding is local-only:

```text
http://127.0.0.1:3001
```

Change this in `.env` if you want LAN access:

```text
GRAFANA_BIND_ADDR=0.0.0.0
```

For enterprise use, prefer putting Grafana behind the normal internal reverse proxy/SSO path rather than exposing it directly.

## Exporter setup

Point a small number of test exporters at the server:

```text
protocol: NetFlow/IPFIX
collector: <internal-server-ip>
UDP port: 2055
```

Document exporter settings:

```text
active timeout
inactive timeout
sampling rate
interfaces/VRFs included
NAT fields, if available
```

## Verification

```bash
docker ps

docker exec flow-clickhouse clickhouse-client --query \
  'SELECT count(), max(received_at) FROM flow_poc.flows'

/opt/flow-poc/analyze_flows.py --since 1h --format text
/opt/flow-poc/analyze_flow_baselines.py --format text
```

## What this does not include

This is an MVP bootstrap, not a full enterprise production build. It does not include:

```text
HA Kafka
HA ClickHouse
SSO/RBAC
TLS everywhere
central secrets manager
enterprise backup/restore
production retention/cost controls
formal CI/CD deployment pipeline
```

Those are Phase 2/production-hardening items.
