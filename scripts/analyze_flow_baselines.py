#!/usr/bin/env python3
"""Rollup-backed baseline/anomaly checks for the flow POC.

Uses flow_poc.flow_rollup_* tables and baseline views. Designed to be useful now
with sparse history and get better automatically as rollup history accumulates.
"""
from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone

DB = "flow_poc"
CH_CONTAINER = "clickhouse"


def ch(sql: str):
    p = subprocess.run(["docker", "exec", "-i", CH_CONTAINER, "clickhouse-client", "--query", sql + " FORMAT JSONEachRow"], text=True, capture_output=True)
    if p.returncode:
        raise RuntimeError(p.stderr or p.stdout)
    return [json.loads(x) for x in p.stdout.splitlines() if x.strip()]


def rb(n):
    try: n=float(n)
    except Exception: return "0 B"
    units=["B","KiB","MiB","GiB","TiB"]
    i=0
    while abs(n)>=1024 and i<len(units)-1:
        n/=1024; i+=1
    return f"{n:.2f} {units[i]}"


def add_readable(rows, field="bytes"):
    for r in rows:
        if field in r:
            r[field + "_readable"] = rb(r[field])
        if "recent_bytes" in r:
            r["recent_bytes_readable"] = rb(r["recent_bytes"])
        if "avg_bytes" in r:
            r["avg_bytes_readable"] = rb(r["avg_bytes"])
        if "p95_bytes" in r:
            r["p95_bytes_readable"] = rb(r["p95_bytes"])
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top-n", type=int, default=10)
    ap.add_argument("--format", choices=["json","text"], default="json")
    ap.add_argument("--min-baseline-buckets", type=int, default=3, help="Minimum hourly baseline buckets before ratio/z-score is treated as mature")
    args = ap.parse_args()
    n=args.top_n

    current_vs_baseline = add_readable(ch(f"""
SELECT
  src_name, src_addr, src_device_type, bytes, flows, baseline_buckets,
  avg_bytes, p95_bytes, max_bytes, z_bytes, ratio_to_avg, ratio_to_p95
FROM {DB}.v_current_hour_device_vs_baseline
ORDER BY ifNull(ratio_to_p95, 0) DESC, bytes DESC
LIMIT {n}
"""))

    volume_anomalies = add_readable(ch(f"""
SELECT
  src_name, src_addr, src_device_type, bytes, flows, baseline_buckets,
  avg_bytes, p95_bytes, z_bytes, ratio_to_avg, ratio_to_p95
FROM {DB}.v_current_hour_device_vs_baseline
WHERE baseline_buckets >= {args.min_baseline_buckets}
  AND bytes > greatest(p95_bytes * 2, avg_bytes * 3, 100000000)
ORDER BY bytes DESC
LIMIT {n}
"""))

    new_ports = add_readable(ch(f"""
SELECT src_name, src_addr, proto, dst_port, recent_bytes, recent_flows
FROM {DB}.v_recent_new_dst_ports
WHERE recent_bytes > 1048576 OR recent_flows >= 10
ORDER BY recent_bytes DESC
LIMIT {n}
"""), "recent_bytes")

    new_external_destinations = add_readable(ch(f"""
SELECT src_name, src_addr, dst_name, dst_addr, proto, dst_port, recent_bytes, recent_flows
FROM {DB}.v_recent_new_external_destinations
WHERE recent_bytes > 1048576 OR recent_flows >= 10
ORDER BY recent_bytes DESC
LIMIT {n}
"""), "recent_bytes")

    recent_top_devices = add_readable(ch(f"""
SELECT src_name, src_addr, sum(bytes) AS bytes, sum(flows) AS flows, sum(packets) AS packets
FROM {DB}.v_rollup_device_5m
WHERE bucket_start >= now() - INTERVAL 1 HOUR
  AND (startsWith(src_addr,'10.') OR startsWith(src_addr,'192.168.') OR startsWith(src_addr,'172.16.') OR startsWith(src_addr,'172.17.') OR startsWith(src_addr,'172.18.') OR startsWith(src_addr,'172.19.') OR startsWith(src_addr,'172.2') OR startsWith(src_addr,'172.30.') OR startsWith(src_addr,'172.31.'))
GROUP BY src_name, src_addr
ORDER BY bytes DESC
LIMIT {n}
"""))

    baseline_coverage = ch(f"""
SELECT
  count() AS hourly_baseline_rows,
  countIf(buckets >= {args.min_baseline_buckets}) AS mature_hourly_baseline_rows,
  min(buckets) AS min_buckets,
  max(buckets) AS max_buckets,
  avg(buckets) AS avg_buckets
FROM {DB}.v_device_hourly_baseline
""")[0]

    rollup_health = ch(f"""
SELECT '5m' AS rollup, count() AS rows, toString(min(bucket_start)) AS first_bucket, toString(max(bucket_start)) AS last_bucket, sum(bytes) AS bytes FROM {DB}.flow_rollup_5m FINAL
UNION ALL
SELECT '1h' AS rollup, count() AS rows, toString(min(bucket_start)) AS first_bucket, toString(max(bucket_start)) AS last_bucket, sum(bytes) AS bytes FROM {DB}.flow_rollup_1h FINAL
UNION ALL
SELECT '1d' AS rollup, count() AS rows, toString(min(bucket_start)) AS first_bucket, toString(max(bucket_start)) AS last_bucket, sum(bytes) AS bytes FROM {DB}.flow_rollup_1d FINAL
""")
    add_readable(rollup_health)

    observations=[]
    anomalies=[]
    risk=0
    mature=int(baseline_coverage.get("mature_hourly_baseline_rows") or 0)
    if mature == 0:
        observations.append("Baseline history is still immature; new-port/new-destination checks work now, but volume anomaly scores will improve after several matching hourly buckets accumulate.")
    if volume_anomalies:
        risk += 20
        anomalies.append({"severity":"warning","type":"volume_vs_baseline","count":len(volume_anomalies),"message":"One or more devices exceed mature hourly volume baseline thresholds."})
    if new_external_destinations:
        risk += min(20, 3*len(new_external_destinations))
        observations.append(f"Recent new external destination tuples detected: {len(new_external_destinations)} top entries returned.")
    if new_ports:
        risk += min(15, 2*len(new_ports))
        observations.append(f"Recent new destination-port tuples detected: {len(new_ports)} top entries returned.")
    risk=min(risk,100)

    result={
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "risk_score": risk,
        "anomalies": anomalies,
        "observations": observations,
        "baseline_coverage": baseline_coverage,
        "rollup_health": rollup_health,
        "current_hour_vs_baseline_top": current_vs_baseline,
        "volume_anomalies": volume_anomalies,
        "new_destination_ports_recent_1h": new_ports,
        "new_external_destinations_recent_1h": new_external_destinations,
        "recent_top_devices_1h": recent_top_devices,
        "metadata": {"database":DB, "min_baseline_buckets":args.min_baseline_buckets, "top_n":n}
    }
    if args.format == "json":
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Baseline/Anomaly Check — Risk {risk}/100")
        print(f"Baseline coverage: hourly rows={baseline_coverage.get('hourly_baseline_rows')} mature={baseline_coverage.get('mature_hourly_baseline_rows')} max_buckets={baseline_coverage.get('max_buckets')}")
        if observations:
            print("\nObservations:")
            for o in observations: print(f"- {o}")
        if anomalies:
            print("\nAnomalies:")
            for a in anomalies: print(f"- [{a['severity']}] {a['message']}")
        print("\nRecent top devices, last 1h:")
        for r in recent_top_devices[:5]: print(f"- {r.get('src_name')} ({r.get('src_addr')}): {r.get('bytes_readable')} / {r.get('flows')} flows")
        print("\nCurrent hour vs baseline:")
        for r in current_vs_baseline[:5]:
            ratio=r.get('ratio_to_p95')
            rs="n/a" if ratio is None else f"{float(ratio):.2f}x p95"
            print(f"- {r.get('src_name')} ({r.get('src_addr')}): {r.get('bytes_readable')} current, baseline p95 {r.get('p95_bytes_readable','n/a')} ({rs}, buckets={r.get('baseline_buckets')})")
        if new_external_destinations:
            print("\nNew external destinations, last 1h:")
            for r in new_external_destinations[:5]:
                dst_label = r.get('dst_name') or r.get('dst_addr')
                print(f"- {r.get('src_name')} -> {dst_label} ({r.get('dst_addr')}) {r.get('proto')}/{r.get('dst_port')}: {r.get('recent_bytes_readable')} / {r.get('recent_flows')} flows")
        if new_ports:
            print("\nNew destination ports, last 1h:")
            for r in new_ports[:5]: print(f"- {r.get('src_name')} {r.get('proto')}/{r.get('dst_port')}: {r.get('recent_bytes_readable')} / {r.get('recent_flows')} flows")

if __name__ == "__main__":
    main()
