# Native server deployment: no Docker/Podman

Use this path when a container runtime is not available and you want a direct install on an internal Linux server.

## What it installs

```text
goflow2 as systemd service
Kafka as systemd service
ClickHouse as native package/service
Grafana OSS as native package/service
Python report/enrichment scripts under /opt/flow-poc
cron jobs for rollups and external IP enrichment
```

## Target platform

Primary target:

```text
Ubuntu/Debian-style Linux server
root/sudo access
internet access to package repositories, Apache Kafka archive, GitHub releases, Grafana packages, ClickHouse packages
```

If the server has restricted internet access, pre-stage packages or set `GOFLOW2_URL` in `.env` to an internally hosted goflow2 artifact.

## One-line install from a cloned repo

```bash
cd flow-intelligence-poc && sudo deploy/native-server/bootstrap.sh
```

That is the cleanest internal-server command because it uses the checked-out repo contents.

## One-line install via GitHub token

Because this repo is private, a true remote one-liner needs a token with read access:

```bash
export GITHUB_TOKEN='[REDACTED]'
curl -fsSL -H "Authorization: Bearer $GITHUB_TOKEN" \
  https://raw.githubusercontent.com/<owner-or-org>/flow-intelligence-poc/main/deploy/native-server/bootstrap-remote.sh \
  | sudo GITHUB_TOKEN="$GITHUB_TOKEN" bash
```

`bootstrap-remote.sh` clones the private repo to `/opt/flow-intelligence-poc-src` and runs `deploy/native-server/bootstrap.sh`.

## Defaults

```text
NetFlow/IPFIX listener: UDP/2056
Kafka broker: 127.0.0.1:9092
ClickHouse native: 127.0.0.1:9000
Grafana: http://127.0.0.1:3001
Scripts: /opt/flow-poc
```

Generated local passwords are stored in:

```text
deploy/native-server/.env
```

## Verification

After pointing one exporter at the server:

```bash
systemctl status flow-goflow2 flow-kafka clickhouse-server grafana-server

clickhouse-client --query \
  'SELECT count(), max(received_at) FROM flow_poc.flows'

/opt/flow-poc/analyze_flows.py --since 1h --format text
/opt/flow-poc/analyze_flow_baselines.py --format text
```

## Enterprise caveats

This is an MVP/direct-server install. It avoids a container runtime dependency, but creates more host-specific state than containers.

Expected friction points:

```text
root/sudo access
package repository access
firewall for UDP/2056
Grafana access path
systemd service ownership
backup/retention
patching responsibility
```

## Positioning

Use this when the organization says no to Docker but still wants fast validation:

```text
Docker is the cleanest MVP path.
Native install is the no-container fallback.
A managed platform deployment is the production-aligned path.
```
