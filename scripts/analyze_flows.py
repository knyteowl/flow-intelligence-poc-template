#!/usr/bin/env python3
"""ClickHouse-backed flow summary for the flow POC.

Examples:
  /opt/flow-poc/analyze_flows.py --since 15m
  /opt/flow-poc/analyze_flows.py --since 1h --format text
  /opt/flow-poc/analyze_flows.py --since "8am" --format text
  /opt/flow-poc/analyze_flows.py --since "yesterday 10pm" --until "today 6am"

Outputs machine-readable JSON by default, with a concise text mode for humans.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

DB = os.environ.get("FLOW_DB", "flow_poc")
TABLE = os.environ.get("FLOW_TABLE", "flows_enriched_external")
CLICKHOUSE_CONTAINER = os.environ.get("CLICKHOUSE_CONTAINER", "clickhouse")
LOCAL_TZ = ZoneInfo(os.environ.get("FLOW_LOCAL_TZ", "America/New_York"))

INTERNAL_PREFIX_SQL = "(" + " OR ".join([
    "startsWith({col},'10.')",
    "startsWith({col},'192.168.')",
    "startsWith({col},'172.16.')",
    "startsWith({col},'172.17.')",
    "startsWith({col},'172.18.')",
    "startsWith({col},'172.19.')",
    "startsWith({col},'172.2')",
    "startsWith({col},'172.30.')",
    "startsWith({col},'172.31.')",
]) + ")"


def ch_query(sql: str):
    cmd = ["docker", "exec", "-i", CLICKHOUSE_CONTAINER, "clickhouse-client", "--query", sql]
    p = subprocess.run(cmd, text=True, capture_output=True)
    if p.returncode != 0:
        raise RuntimeError(f"ClickHouse query failed: {p.stderr or p.stdout}\nSQL: {sql}")
    text = p.stdout.strip()
    if not text:
        return []
    try:
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Could not parse ClickHouse JSONEachRow output: {e}\nOutput: {text[:500]}")


def q(s: str) -> str:
    return "'" + str(s).replace("'", "\\'") + "'"


def parse_time_expr(expr: str | None, *, default: datetime | None = None) -> datetime:
    if not expr:
        if default is None:
            raise ValueError("missing time expression")
        return default
    raw = expr.strip().lower()
    now = datetime.now(LOCAL_TZ)

    if raw in ("now", "current"):
        return now

    m = re.fullmatch(r"(\d+)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)", raw)
    if m:
        n = int(m.group(1)); unit = m.group(2)
        if unit.startswith("m"):
            return now - timedelta(minutes=n)
        if unit.startswith("h"):
            return now - timedelta(hours=n)
        return now - timedelta(days=n)

    m = re.fullmatch(r"(\d+)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)\s+ago", raw)
    if m:
        return parse_time_expr(m.group(1) + m.group(2))

    day = now.date()
    rest = raw
    if raw.startswith("today "):
        rest = raw[len("today "):].strip()
    elif raw.startswith("yesterday "):
        day = (now - timedelta(days=1)).date()
        rest = raw[len("yesterday "):].strip()

    m = re.fullmatch(r"(\d{1,2})(?::(\d{2}))?\s*(am|pm)?", rest)
    if m:
        hour = int(m.group(1)); minute = int(m.group(2) or 0); ampm = m.group(3)
        if ampm == "pm" and hour != 12:
            hour += 12
        if ampm == "am" and hour == 12:
            hour = 0
        if not ampm and hour > 23:
            raise ValueError(f"invalid hour in {expr!r}")
        dt = datetime(day.year, day.month, day.day, hour, minute, tzinfo=LOCAL_TZ)
        # Bare times in the future usually mean yesterday's time window.
        if not raw.startswith("today ") and not raw.startswith("yesterday ") and dt > now:
            dt -= timedelta(days=1)
        return dt

    # ISO-ish fallback. Naive timestamps are interpreted in local timezone.
    try:
        dt = datetime.fromisoformat(expr.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=LOCAL_TZ)
        return dt.astimezone(LOCAL_TZ)
    except Exception as e:
        raise ValueError(f"Unsupported time expression {expr!r}. Try 15m, 1h, 8am, today 8am, or ISO timestamp.") from e


def ch_time(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def readable_bytes(n) -> str:
    try:
        n = float(n)
    except Exception:
        return "0 B"
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    i = 0
    while abs(n) >= 1024 and i < len(units) - 1:
        n /= 1024.0; i += 1
    return f"{n:.2f} {units[i]}"


def readable_bps(n) -> str:
    try:
        n = float(n)
    except Exception:
        return "0 bps"
    units = ["bps", "Kbps", "Mbps", "Gbps", "Tbps"]
    i = 0
    while abs(n) >= 1000 and i < len(units) - 1:
        n /= 1000.0; i += 1
    return f"{n:.2f} {units[i]}"


def rows(sql):
    return ch_query(sql + " FORMAT JSONEachRow")


def analyze(since_dt: datetime, until_dt: datetime, top_n: int):
    since = ch_time(since_dt)
    until = ch_time(until_dt)
    where = f"received_at >= toDateTime64({q(since)}, 9) AND received_at < toDateTime64({q(until)}, 9)"
    internal_src = INTERNAL_PREFIX_SQL.format(col="src_addr")
    internal_dst = INTERNAL_PREFIX_SQL.format(col="dst_addr")
    external_dst = f"NOT {internal_dst}"
    duration = max((until_dt - since_dt).total_seconds(), 1)

    overview = rows(f"""
SELECT
  count() AS flows,
  uniqExact(src_addr) AS unique_sources,
  uniqExact(dst_addr) AS unique_destinations,
  uniqExact(sampler_address) AS exporters,
  sum(bytes) AS total_bytes,
  sum(packets) AS total_packets,
  min(received_at) AS first_seen,
  max(received_at) AS last_seen,
  countIf(proto='UDP' AND dst_port=443) AS udp443_flows,
  sumIf(bytes, proto='UDP' AND dst_port=443) AS udp443_bytes,
  countIf(dst_port=22 OR src_port=22) AS ssh_flows,
  sumIf(bytes, dst_port=22 OR src_port=22) AS ssh_bytes
FROM {DB}.{TABLE}
WHERE {where}
""")[0]

    # Add derived values in Python to avoid integer division surprises.
    total_bytes = int(overview.get("total_bytes") or 0)
    total_packets = int(overview.get("total_packets") or 0)
    overview["duration_seconds"] = duration
    overview["avg_bits_per_sec"] = round((total_bytes * 8) / duration, 2)
    overview["avg_packets_per_sec"] = round(total_packets / duration, 2)
    overview["total_bytes_readable"] = readable_bytes(total_bytes)
    overview["avg_bits_per_sec_readable"] = readable_bps(overview["avg_bits_per_sec"])

    top_talkers = rows(f"""
SELECT src_name, src_addr, src_device_type, sum(bytes) AS bytes, sum(packets) AS packets, count() AS flows
FROM {DB}.{TABLE}
WHERE {where}
GROUP BY src_name, src_addr, src_device_type
ORDER BY bytes DESC
LIMIT {top_n}
""")
    top_destinations = rows(f"""
SELECT dst_org_or_name AS dst_name, dst_addr, dst_device_type, dst_asn, dst_as_name, dst_country_code, dst_reverse_dns, sum(bytes) AS bytes, sum(packets) AS packets, count() AS flows
FROM {DB}.{TABLE}
WHERE {where}
GROUP BY dst_name, dst_addr, dst_device_type, dst_asn, dst_as_name, dst_country_code, dst_reverse_dns
ORDER BY bytes DESC
LIMIT {top_n}
""")
    top_conversations = rows(f"""
SELECT src_org_or_name AS src_name, src_addr, dst_org_or_name AS dst_name, dst_addr, dst_asn, dst_as_name, dst_country_code, dst_reverse_dns, proto, dst_port, sum(bytes) AS bytes, sum(packets) AS packets, count() AS flows
FROM {DB}.{TABLE}
WHERE {where}
GROUP BY src_name, src_addr, dst_name, dst_addr, dst_asn, dst_as_name, dst_country_code, dst_reverse_dns, proto, dst_port
ORDER BY bytes DESC
LIMIT {top_n}
""")
    top_ports = rows(f"""
SELECT proto, dst_port, sum(bytes) AS bytes, sum(packets) AS packets, count() AS flows, uniqExact(src_addr) AS sources, uniqExact(dst_addr) AS destinations
FROM {DB}.{TABLE}
WHERE {where}
GROUP BY proto, dst_port
ORDER BY bytes DESC
LIMIT {top_n}
""")
    large_outbound = rows(f"""
SELECT src_org_or_name AS src_name, src_addr, dst_org_or_name AS dst_name, dst_addr, dst_asn, dst_as_name, dst_country_code, dst_reverse_dns, proto, dst_port, sum(bytes) AS bytes, sum(packets) AS packets, count() AS flows
FROM {DB}.{TABLE}
WHERE {where} AND {internal_src} AND {external_dst}
GROUP BY src_name, src_addr, dst_name, dst_addr, dst_asn, dst_as_name, dst_country_code, dst_reverse_dns, proto, dst_port
ORDER BY bytes DESC
LIMIT {top_n}
""")
    nonstandard_external = rows(f"""
SELECT src_org_or_name AS src_name, src_addr, dst_org_or_name AS dst_name, dst_addr, dst_asn, dst_as_name, dst_country_code, dst_reverse_dns, proto, dst_port, sum(bytes) AS bytes, count() AS flows
FROM {DB}.{TABLE}
WHERE {where} AND {internal_src} AND {external_dst} AND dst_port NOT IN (80,443,53,123,22)
GROUP BY src_name, src_addr, dst_name, dst_addr, dst_asn, dst_as_name, dst_country_code, dst_reverse_dns, proto, dst_port
ORDER BY bytes DESC
LIMIT {top_n}
""")
    ssh_activity = rows(f"""
SELECT src_org_or_name AS src_name, src_addr, dst_org_or_name AS dst_name, dst_addr, dst_asn, dst_as_name, dst_country_code, dst_reverse_dns, proto, src_port, dst_port, sum(bytes) AS bytes, count() AS flows
FROM {DB}.{TABLE}
WHERE {where} AND (src_port=22 OR dst_port=22)
GROUP BY src_name, src_addr, dst_name, dst_addr, dst_asn, dst_as_name, dst_country_code, dst_reverse_dns, proto, src_port, dst_port
ORDER BY bytes DESC
LIMIT {top_n}
""")
    pipeline = rows(f"""
SELECT
  now() AS checked_at,
  max(received_at) AS latest_flow_received_at,
  dateDiff('second', max(received_at), now()) AS ingest_lag_seconds,
  uniqExact(sampler_address) AS exporter_count,
  count() AS recent_rows
FROM {DB}.{TABLE}
WHERE received_at > now() - INTERVAL 15 MINUTE
""")[0]

    anomalies = []
    observations = []
    risk_score = 0

    flows = int(overview.get("flows") or 0)
    if flows == 0:
        anomalies.append({"severity": "warning", "type": "no_flows", "message": "No flows found in the requested window."})
        risk_score += 20
    lag = int(pipeline.get("ingest_lag_seconds") or 0)
    if lag > 300:
        anomalies.append({"severity": "warning", "type": "ingestion_stale", "message": f"Latest flow is {lag} seconds old."})
        risk_score += 25
    exporters = int(pipeline.get("exporter_count") or 0)
    if exporters == 0:
        anomalies.append({"severity": "critical", "type": "no_exporter", "message": "No exporter observed in the last 15 minutes."})
        risk_score += 40

    udp443_bytes = int(overview.get("udp443_bytes") or 0)
    if total_bytes and udp443_bytes / total_bytes > 0.50:
        observations.append(f"UDP/443 represents {udp443_bytes / total_bytes:.0%} of observed bytes; often consistent with QUIC/video streaming.")

    for r in large_outbound[:5]:
        b = int(r.get("bytes") or 0)
        if b > 500 * 1024 * 1024:
            anomalies.append({
                "severity": "info",
                "type": "large_outbound_transfer",
                "message": f"{r.get('src_name') or r.get('src_addr')} sent {readable_bytes(b)} to external {(r.get('dst_name') or r.get('dst_addr'))} ({r.get('dst_addr')}):{r.get('dst_port')}/{r.get('proto')}."
            })
            risk_score += 5
    if nonstandard_external:
        observations.append(f"Observed {len(nonstandard_external)} non-standard external destination port entries in top {top_n}.")
    if ssh_activity:
        observations.append(f"Observed SSH-related activity in requested window: {len(ssh_activity)} top entries returned.")

    risk_score = min(risk_score, 100)
    for section in [top_talkers, top_destinations, top_conversations, top_ports, large_outbound, nonstandard_external, ssh_activity]:
        for r in section:
            if "bytes" in r:
                r["bytes_readable"] = readable_bytes(r["bytes"])

    return {
        "window": {
            "since_local": since_dt.isoformat(),
            "until_local": until_dt.isoformat(),
            "since_utc": since,
            "until_utc": until,
            "duration_seconds": duration,
        },
        "summary": overview,
        "top_talkers": top_talkers,
        "top_destinations": top_destinations,
        "top_conversations": top_conversations,
        "top_ports": top_ports,
        "large_outbound_transfers": large_outbound,
        "nonstandard_external_ports": nonstandard_external,
        "ssh_activity": ssh_activity,
        "pipeline_health": pipeline,
        "anomalies": anomalies,
        "observations": observations,
        "risk_score": risk_score,
        "metadata": {"database": DB, "table": TABLE, "timezone": str(LOCAL_TZ), "top_n": top_n},
    }


def print_text(result):
    s = result["summary"]
    print(f"Flow Summary ({result['window']['since_local']} -> {result['window']['until_local']})")
    print(f"Risk score: {result['risk_score']}/100")
    print(f"Flows: {s.get('flows')} | Bytes: {s.get('total_bytes_readable')} | Avg throughput: {s.get('avg_bits_per_sec_readable')} | Exporters: {s.get('exporters')}")
    print(f"Rows latest: {result['pipeline_health'].get('latest_flow_received_at')} | ingest lag: {result['pipeline_health'].get('ingest_lag_seconds')}s")
    if result["observations"]:
        print("\nObservations:")
        for o in result["observations"]:
            print(f"- {o}")
    if result["anomalies"]:
        print("\nSignals:")
        for a in result["anomalies"]:
            print(f"- [{a.get('severity')}] {a.get('message')}")
    print("\nTop talkers:")
    for r in result["top_talkers"][:5]:
        print(f"- {r.get('src_name')} ({r.get('src_addr')}): {r.get('bytes_readable')} / {r.get('flows')} flows")
    print("\nTop destinations:")
    for r in result["top_destinations"][:5]:
        print(f"- {r.get('dst_name')} ({r.get('dst_addr')}): {r.get('bytes_readable')} / {r.get('flows')} flows")
    print("\nTop conversations:")
    for r in result["top_conversations"][:5]:
        print(f"- {r.get('src_name')} -> {r.get('dst_name')} {r.get('proto')}/{r.get('dst_port')}: {r.get('bytes_readable')}")


def main(argv=None):
    ap = argparse.ArgumentParser(description="Summarize flow data from ClickHouse")
    ap.add_argument("--since", default="15m", help="Start time: 15m, 1h, 8am, today 8am, yesterday 10pm, or ISO timestamp")
    ap.add_argument("--until", default="now", help="End time, default now")
    ap.add_argument("--top-n", type=int, default=10)
    ap.add_argument("--format", choices=["json", "text"], default="json")
    args = ap.parse_args(argv)
    try:
        until = parse_time_expr(args.until, default=datetime.now(LOCAL_TZ))
        since = parse_time_expr(args.since)
        if since >= until:
            raise ValueError("--since must be earlier than --until")
        result = analyze(since, until, args.top_n)
        if args.format == "text":
            print_text(result)
        else:
            print(json.dumps(result, indent=2, default=str))
        return 0
    except Exception as e:
        err = {"summary": {"flows": 0}, "anomalies": [{"severity": "critical", "type": "script_error", "message": str(e)}], "observations": [], "risk_score": 100, "metadata": {"status": "failed"}}
        print(json.dumps(err, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
