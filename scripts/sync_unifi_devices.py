#!/usr/bin/env python3
"""Sync UniFi device/client inventory into ClickHouse for the flow POC.

Supports UniFi Network Integration API base URLs such as:
  https://<console-ip-or-host>/proxy/network/integration

The cloud Site Manager API (https://api.ui.com) currently exposes hosts/sites but
not full client inventory via the tested v1 endpoints; this script detects that
and exits with a clear message.
"""
import json
import os
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

ENV_FILE = "/opt/flow-poc/secrets/unifi.env"
CLICKHOUSE_CONTAINER = os.environ.get("CLICKHOUSE_CONTAINER", "clickhouse")
DB = os.environ.get("CLICKHOUSE_DB", "flow_poc")


def load_env(path):
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            v = v.strip().strip('"').strip("'")
            os.environ.setdefault(k.strip(), v)


def request_json(base, version, endpoint, key, verify_ssl=True, params=None):
    endpoint = endpoint.lstrip("/")
    url = f"{base.rstrip('/')}/{version.strip('/')}/{endpoint}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"X-API-Key": key, "X-API-KEY": key, "Accept": "application/json"})
    ctx = None if verify_ssl else ssl._create_unverified_context()
    try:
        with urllib.request.urlopen(req, timeout=30, context=ctx) as r:
            return r.status, json.loads(r.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:500]
        return e.code, {"error": body}


def fetch_all(base, version, endpoint, key, verify_ssl=True):
    out = []
    offset = 0
    limit = 200
    while True:
        status, data = request_json(base, version, endpoint, key, verify_ssl, {"offset": offset, "limit": limit})
        if status != 200:
            raise RuntimeError(f"GET /{version}/{endpoint} returned HTTP {status}: {data.get('error','')[:200]}")
        rows = data.get("data", data if isinstance(data, list) else [])
        if not isinstance(rows, list):
            rows = []
        out.extend(rows)
        count = data.get("count", len(rows)) if isinstance(data, dict) else len(rows)
        total = data.get("totalCount", 0) if isinstance(data, dict) else 0
        if not rows or count <= 0 or (total and len(out) >= total) or len(rows) < limit:
            break
        offset += count
    return out


def first(*vals):
    for v in vals:
        if v not in (None, "", [], {}):
            return str(v)
    return ""


def norm_ts(v):
    if v in (None, "", 0):
        return "1970-01-01 00:00:00"
    try:
        if isinstance(v, (int, float)):
            if v > 10_000_000_000:
                v = v / 1000
            return datetime.fromtimestamp(v, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        s = str(v).replace("Z", "+00:00")
        return datetime.fromisoformat(s).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "1970-01-01 00:00:00"


def normalize_client(row, site_id, site_name):
    ip = first(row.get("ipAddress"), row.get("ip"), row.get("lastIp"), row.get("fixedIp"), row.get("networkAddress"))
    mac = first(row.get("macAddress"), row.get("mac"), row.get("id"))
    name = first(row.get("name"), row.get("displayName"), row.get("hostname"), row.get("hostName"), row.get("clientName"), row.get("userName"), mac, ip)
    return {
        "ip": ip,
        "mac": mac,
        "name": name,
        "hostname": first(row.get("hostname"), row.get("hostName")),
        "vendor": first(row.get("vendor"), row.get("manufacturer"), row.get("oui")),
        "device_type": first(row.get("type"), row.get("deviceType"), row.get("osName"), "client"),
        "network": first(row.get("network"), row.get("networkName"), row.get("vlan"), row.get("ssid")),
        "site_id": site_id,
        "site_name": site_name,
        "source": "unifi-client",
        "last_seen": norm_ts(first(row.get("lastSeen"), row.get("last_seen"), row.get("lastConnectionTime"), row.get("lastActivity"))),
        "raw_json": json.dumps(row, separators=(",", ":"), ensure_ascii=False),
    }


def normalize_device(row, site_id, site_name):
    ip = first(row.get("ipAddress"), row.get("ip"), row.get("lastIp"))
    mac = first(row.get("macAddress"), row.get("mac"), row.get("id"))
    name = first(row.get("name"), row.get("displayName"), row.get("model"), mac, ip)
    return {
        "ip": ip,
        "mac": mac,
        "name": name,
        "hostname": first(row.get("hostname"), row.get("hostName")),
        "vendor": first("Ubiquiti"),
        "device_type": first(row.get("type"), row.get("model"), "unifi-device"),
        "network": first(row.get("network"), row.get("networkName")),
        "site_id": site_id,
        "site_name": site_name,
        "source": "unifi-device",
        "last_seen": norm_ts(first(row.get("lastSeen"), row.get("last_seen"), row.get("lastConnectionTime"))),
        "raw_json": json.dumps(row, separators=(",", ":"), ensure_ascii=False),
    }


def clickhouse_query(sql, input_data=None):
    cmd = ["docker", "exec", "-i", CLICKHOUSE_CONTAINER, "clickhouse-client", "--multiquery"]
    p = subprocess.run(cmd, input=sql if input_data is None else sql + input_data, text=True, capture_output=True)
    if p.returncode:
        raise RuntimeError(p.stderr or p.stdout)
    return p.stdout


def ensure_schema():
    clickhouse_query(f"""
CREATE TABLE IF NOT EXISTS {DB}.device_inventory
(
    ip String,
    mac String,
    name String,
    hostname String,
    vendor String,
    device_type String,
    network String,
    site_id String,
    site_name String,
    source String,
    last_seen DateTime,
    updated_at DateTime DEFAULT now(),
    raw_json String
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (ip, mac, source);

CREATE OR REPLACE VIEW {DB}.device_inventory_current AS
SELECT
    ip,
    argMax(mac, updated_at) AS mac,
    argMax(name, updated_at) AS name,
    argMax(hostname, updated_at) AS hostname,
    argMax(vendor, updated_at) AS vendor,
    argMax(device_type, updated_at) AS device_type,
    argMax(network, updated_at) AS network,
    argMax(site_id, updated_at) AS site_id,
    argMax(site_name, updated_at) AS site_name,
    argMax(source, updated_at) AS source,
    max(last_seen) AS last_seen,
    max(updated_at) AS latest_updated_at
FROM {DB}.device_inventory
WHERE ip != ''
GROUP BY ip;

CREATE OR REPLACE VIEW {DB}.flows_enriched AS
SELECT
    f.*,
    coalesce(nullIf(src.name, ''), f.src_addr) AS src_name,
    src.mac AS src_mac,
    src.vendor AS src_vendor,
    src.device_type AS src_device_type,
    src.network AS src_network,
    coalesce(nullIf(dst.name, ''), f.dst_addr) AS dst_name,
    dst.mac AS dst_mac,
    dst.vendor AS dst_vendor,
    dst.device_type AS dst_device_type,
    dst.network AS dst_network
FROM {DB}.flows AS f
LEFT JOIN {DB}.device_inventory_current AS src ON f.src_addr = src.ip
LEFT JOIN {DB}.device_inventory_current AS dst ON f.dst_addr = dst.ip;
""")


def insert_rows(rows):
    if not rows:
        return 0
    payload = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)
    sql = f"INSERT INTO {DB}.device_inventory FORMAT JSONEachRow\n"
    clickhouse_query(sql, payload)
    return len(rows)


def main():
    load_env(ENV_FILE)
    key = os.environ.get("UNIFI_API_KEY")
    base = os.environ.get("UNIFI_API_BASE", "https://api.ui.com").rstrip("/")
    version = os.environ.get("UNIFI_API_VERSION", "v1")
    verify_ssl = os.environ.get("UNIFI_VERIFY_SSL", "true").lower() not in ("0", "false", "no")
    if not key:
        print("ERROR: UNIFI_API_KEY missing", file=sys.stderr)
        return 2

    ensure_schema()

    # Two supported modes:
    # 1) Local/Network Integration API base: https://<console>/proxy/network/integration
    #    endpoints: /v1/sites/{site_id}/clients
    # 2) Cloud connector proxy: https://api.ui.com
    #    endpoints: /v1/connector/consoles/{consoleId}/proxy/network/integration/v1/sites/{site_id}/clients
    site_entries = []
    if "api.ui.com" in base:
        status, hosts_resp = request_json(base, version, "hosts", key, verify_ssl)
        if status != 200:
            print(f"ERROR: Could not list consoles from {base}/{version}/hosts: HTTP {status}", file=sys.stderr)
            return 2
        hosts = hosts_resp.get("data", hosts_resp if isinstance(hosts_resp, list) else [])
        for host in hosts:
            console_id = first(host.get("id"))
            if not console_id:
                continue
            console_quoted = urllib.parse.quote(console_id, safe="")
            sites_endpoint = f"connector/consoles/{console_quoted}/proxy/network/integration/v1/sites"
            status, sites_resp = request_json(base, version, sites_endpoint, key, verify_ssl)
            if status != 200:
                continue
            for s in sites_resp.get("data", sites_resp if isinstance(sites_resp, list) else []):
                site_id = first(s.get("id"), s.get("internalReference"), s.get("name"))
                site_name = first(s.get("name"), s.get("internalReference"), site_id)
                site_entries.append({
                    "site_id": site_id,
                    "site_name": site_name,
                    "endpoint_prefix": f"connector/consoles/{console_quoted}/proxy/network/integration/v1/sites/{site_id}",
                })
    else:
        status, sites_resp = request_json(base, version, "sites", key, verify_ssl)
        if status != 200:
            print(f"ERROR: Could not list sites from {base}/{version}/sites: HTTP {status}", file=sys.stderr)
            return 2
        for s in sites_resp.get("data", sites_resp if isinstance(sites_resp, list) else []):
            site_id = first(s.get("id"), s.get("siteId"), s.get("name"), s.get("meta", {}).get("name"))
            site_name = first(s.get("name"), s.get("meta", {}).get("name"), s.get("meta", {}).get("desc"), site_id)
            site_entries.append({"site_id": site_id, "site_name": site_name, "endpoint_prefix": f"sites/{site_id}"})

    rows = []
    failures = []
    for s in site_entries:
        site_id = s["site_id"]
        site_name = s["site_name"]
        for kind, normalizer in [("clients", normalize_client), ("devices", normalize_device)]:
            try:
                items = fetch_all(base, version, f"{s['endpoint_prefix']}/{kind}", key, verify_ssl)
                for item in items:
                    r = normalizer(item, site_id, site_name)
                    if r["ip"] or r["mac"]:
                        rows.append(r)
            except Exception as e:
                failures.append(f"site={site_name} {kind}: {e}")
    inserted = insert_rows(rows)
    print(json.dumps({"base": base, "version": version, "sites": len(site_entries), "rows_inserted": inserted, "failures": failures[:5]}, indent=2))
    return 0 if inserted > 0 else 3

if __name__ == "__main__":
    raise SystemExit(main())
