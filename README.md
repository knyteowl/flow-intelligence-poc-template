# flow-intelligence-poc

Network flow intelligence proof-of-concept for turning raw flow telemetry into operator-friendly device, external-identity, baseline, anomaly, and dashboard views.

## What this contains

```text
scripts/                 Python automation/reporting jobs
sql/                     ClickHouse schema, rollups, enrichment, and baseline views
grafana/dashboards/      Provisioned Grafana dashboard JSON
docs/                    Deployment and operations notes
config/                  Example config/templates only; no secrets
```

## POC architecture

```text
GoFlow2 / flow exporter
  -> Kafka topic flows.raw
  -> ClickHouse flow_poc.flows
  -> network inventory enrichment
  -> external ASN/org/PTR enrichment
  -> rollup tables and baseline views
  -> Grafana dashboards + scheduled reports
```

## Major capabilities

- Network inventory enrichment for internal source/destination names.
- External IP enrichment with ASN, organization, BGP prefix, country, and PTR.
- ClickHouse rollups at 5-minute, 1-hour, and 1-day grain.
- Baseline/anomaly views for current-hour-vs-history, new external destinations, and new destination ports.
- Grafana dashboards:
  - Flow POC overview
  - Device Drilldown
  - Security / Anomaly
  - Pipeline Health
- Python scripts for sync, rollup refresh, flow summaries, and baseline reports.

## Enterprise alignment

Production-aligned components:

- ClickHouse for high-volume flow analytics and rollups.
- Kafka-style ingest path for decoupled telemetry ingestion.
- Grafana dashboards for network-operator visibility.
- Metadata enrichment separated from raw flow ingest.
- Rollup-backed baselines instead of expensive raw-flow history scans.

POC/local stand-ins:

- Single-node ClickHouse/Kafka/Grafana deployment.
- Root cron instead of centralized orchestration.
- Local scripts instead of production CI/CD packaging.
- Team Cymru/PTR enrichment only; no enterprise DNS/SNI/proxy-log correlation yet.

Production gaps to address:

- HA ClickHouse/Kafka and retention management.
- Authentication/authorization for Grafana and data sources.
- Secrets management through a vault or platform-native secret store.
- CI/CD deployment pipeline.
- Known-good/watchlist tagging.

## Secrets policy

This repo intentionally excludes secrets. Do not commit:

- Inventory/API keys
- GitHub tokens
- ClickHouse credentials
- Grafana admin credentials
- SSH keys
- `.env` files
- logs containing sensitive values

Use `config/unifi.env.example` as a template for controller/inventory credentials and keep real values in a root-only file such as:

```bash
/opt/flow-poc/secrets/unifi.env
chmod 600 /opt/flow-poc/secrets/unifi.env
```

## Current operational note

For long-lived HTTPS/QUIC/proxy flows, raw IPFIX records may be exported every active timeout interval and can look like periodic spikes. The 5-minute rollup refresh distributes bytes and packets across the flow duration so short-window throughput is less sensitive to exporter active-timeout artifacts. The POC also maintains a 5-minute bidirectional conversation table/view that normalizes endpoint ordering and service port selection for operator investigations.

## Public template note

This repository is a sanitized template. Replace example repo URLs, inventory sources, retention values, dashboard defaults, and deployment settings with values appropriate for your own environment. Do not commit real credentials, internal hostnames, screenshots, flow exports, or environment-specific expected-traffic notes.
