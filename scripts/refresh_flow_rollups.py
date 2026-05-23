#!/usr/bin/env python3
"""Refresh ClickHouse rollup tables for the flow POC.

This is intentionally batch/idempotent for the POC: delete recent buckets, then
reinsert aggregate rows from flow_poc.flows_enriched. That keeps device names
current after UniFi inventory refreshes and avoids complex MV state during schema
iteration.
"""
from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone

DB = "flow_poc"
SRC = "flows_enriched"
CH_CONTAINER = "clickhouse"


def run_ch(sql: str):
    p = subprocess.run(
        ["docker", "exec", "-i", CH_CONTAINER, "clickhouse-client", "--multiquery"],
        input=sql,
        text=True,
        capture_output=True,
    )
    if p.returncode != 0:
        raise RuntimeError(p.stderr or p.stdout)
    return p.stdout


def refresh_5m(hours: int):
    return f"""
ALTER TABLE {DB}.flow_rollup_5m DELETE WHERE bucket_start >= toStartOfInterval(now() - INTERVAL {hours} HOUR, INTERVAL 5 MINUTE);
INSERT INTO {DB}.flow_rollup_5m
SELECT
    toDateTime(bucket_ts) AS bucket_start,
    src_addr,
    any(src_name) AS src_name,
    any(src_device_type) AS src_device_type,
    dst_addr,
    any(dst_name) AS dst_name,
    any(dst_device_type) AS dst_device_type,
    proto,
    toUInt16(dst_port) AS dst_port,
    toUInt64(sum(toUInt64(round(bytes * overlap_seconds / duration_seconds)))) AS bytes,
    toUInt64(sum(toUInt64(round(packets * overlap_seconds / duration_seconds)))) AS packets,
    toUInt64(count()) AS flows,
    min(flow_start_norm) AS first_seen,
    max(flow_end_norm) AS last_seen,
    now() AS updated_at
FROM
(
    SELECT
        *,
        arrayJoin(range(
            toUInt32(toUnixTimestamp(toStartOfInterval(flow_start_norm, INTERVAL 5 MINUTE))),
            toUInt32(toUnixTimestamp(toStartOfInterval(flow_end_norm, INTERVAL 5 MINUTE))) + 300,
            300
        )) AS bucket_ts,
        greatest(
            0,
            least(toUnixTimestamp(flow_end_norm), bucket_ts + 300) - greatest(toUnixTimestamp(flow_start_norm), bucket_ts)
        ) AS overlap_seconds,
        greatest(1, dateDiff('second', flow_start_norm, flow_end_norm)) AS duration_seconds
    FROM
    (
        SELECT
            *,
            if(flow_end > flow_start, flow_start, received_at) AS flow_start_norm,
            if(flow_end > flow_start, flow_end, received_at + INTERVAL 1 SECOND) AS flow_end_norm
        FROM {DB}.{SRC}
        WHERE received_at >= now() - INTERVAL {hours} HOUR
    )
)
WHERE overlap_seconds > 0
GROUP BY bucket_start, src_addr, dst_addr, proto, dst_port;
"""


def refresh_conversations_5m(hours: int):
    return f"""
ALTER TABLE {DB}.flow_conversation_5m DELETE WHERE bucket_start >= toStartOfInterval(now() - INTERVAL {hours} HOUR, INTERVAL 5 MINUTE);
INSERT INTO {DB}.flow_conversation_5m
SELECT
    toDateTime(bucket_ts) AS bucket_start,
    if(src_addr <= dst_addr, src_addr, dst_addr) AS endpoint_a_addr,
    any(if(src_addr <= dst_addr, src_name, dst_name)) AS endpoint_a_name,
    any(if(src_addr <= dst_addr, src_device_type, dst_device_type)) AS endpoint_a_device_type,
    if(src_addr <= dst_addr, dst_addr, src_addr) AS endpoint_b_addr,
    any(if(src_addr <= dst_addr, dst_name, src_name)) AS endpoint_b_name,
    any(if(src_addr <= dst_addr, dst_device_type, src_device_type)) AS endpoint_b_device_type,
    proto,
    toUInt16(if(src_port > 32767 AND dst_port <= 32767, dst_port, if(dst_port > 32767 AND src_port <= 32767, src_port, least(src_port, dst_port)))) AS service_port,
    toUInt64(sum(toUInt64(round(bytes * overlap_seconds / duration_seconds)))) AS bytes,
    toUInt64(sum(toUInt64(round(packets * overlap_seconds / duration_seconds)))) AS packets,
    toUInt64(count()) AS flows,
    min(flow_start_norm) AS first_seen,
    max(flow_end_norm) AS last_seen,
    now() AS updated_at
FROM
(
    SELECT
        *,
        arrayJoin(range(
            toUInt32(toUnixTimestamp(toStartOfInterval(flow_start_norm, INTERVAL 5 MINUTE))),
            toUInt32(toUnixTimestamp(toStartOfInterval(flow_end_norm, INTERVAL 5 MINUTE))) + 300,
            300
        )) AS bucket_ts,
        greatest(
            0,
            least(toUnixTimestamp(flow_end_norm), bucket_ts + 300) - greatest(toUnixTimestamp(flow_start_norm), bucket_ts)
        ) AS overlap_seconds,
        greatest(1, dateDiff('second', flow_start_norm, flow_end_norm)) AS duration_seconds
    FROM
    (
        SELECT
            *,
            if(flow_end > flow_start, flow_start, received_at) AS flow_start_norm,
            if(flow_end > flow_start, flow_end, received_at + INTERVAL 1 SECOND) AS flow_end_norm
        FROM {DB}.{SRC}
        WHERE received_at >= now() - INTERVAL {hours} HOUR
    )
)
WHERE overlap_seconds > 0
GROUP BY bucket_start, endpoint_a_addr, endpoint_b_addr, proto, service_port;
"""


def refresh_1h(days: int):
    return f"""
ALTER TABLE {DB}.flow_rollup_1h DELETE WHERE bucket_start >= toStartOfHour(now() - INTERVAL {days} DAY);
INSERT INTO {DB}.flow_rollup_1h
SELECT
    toStartOfHour(received_at) AS bucket_start,
    src_addr,
    any(src_name) AS src_name,
    any(src_device_type) AS src_device_type,
    dst_addr,
    any(dst_name) AS dst_name,
    any(dst_device_type) AS dst_device_type,
    proto,
    toUInt16(dst_port) AS dst_port,
    toUInt64(sum(bytes)) AS bytes,
    toUInt64(sum(packets)) AS packets,
    toUInt64(count()) AS flows,
    min(received_at) AS first_seen,
    max(received_at) AS last_seen,
    now() AS updated_at
FROM {DB}.{SRC}
WHERE received_at >= now() - INTERVAL {days} DAY
GROUP BY bucket_start, src_addr, dst_addr, proto, dst_port;
"""


def refresh_1d(days: int):
    return f"""
ALTER TABLE {DB}.flow_rollup_1d DELETE WHERE bucket_start >= toDate(now() - INTERVAL {days} DAY);
INSERT INTO {DB}.flow_rollup_1d
SELECT
    toDate(received_at) AS bucket_start,
    src_addr,
    any(src_name) AS src_name,
    any(src_device_type) AS src_device_type,
    dst_addr,
    any(dst_name) AS dst_name,
    any(dst_device_type) AS dst_device_type,
    proto,
    toUInt16(dst_port) AS dst_port,
    toUInt64(sum(bytes)) AS bytes,
    toUInt64(sum(packets)) AS packets,
    toUInt64(count()) AS flows,
    min(received_at) AS first_seen,
    max(received_at) AS last_seen,
    now() AS updated_at
FROM {DB}.{SRC}
WHERE received_at >= now() - INTERVAL {days} DAY
GROUP BY bucket_start, src_addr, dst_addr, proto, dst_port;
"""


def counts_json():
    sql = f"""
SELECT '5m' AS rollup, count() AS rows, toString(min(bucket_start)) AS first_bucket, toString(max(bucket_start)) AS last_bucket, sum(bytes) AS bytes FROM {DB}.flow_rollup_5m FINAL
UNION ALL
SELECT 'conversation_5m' AS rollup, count() AS rows, toString(min(bucket_start)) AS first_bucket, toString(max(bucket_start)) AS last_bucket, sum(bytes) AS bytes FROM {DB}.flow_conversation_5m FINAL
UNION ALL
SELECT '1h' AS rollup, count() AS rows, toString(min(bucket_start)) AS first_bucket, toString(max(bucket_start)) AS last_bucket, sum(bytes) AS bytes FROM {DB}.flow_rollup_1h FINAL
UNION ALL
SELECT '1d' AS rollup, count() AS rows, toString(min(bucket_start)) AS first_bucket, toString(max(bucket_start)) AS last_bucket, sum(bytes) AS bytes FROM {DB}.flow_rollup_1d FINAL
FORMAT JSONEachRow
"""
    p = subprocess.run(["docker", "exec", "-i", CH_CONTAINER, "clickhouse-client", "--query", sql], text=True, capture_output=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr or p.stdout)
    return [json.loads(x) for x in p.stdout.splitlines() if x.strip()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--five-minute-hours", type=int, default=36, help="Refresh this many recent hours for 5m rollups")
    ap.add_argument("--hourly-days", type=int, default=14, help="Refresh this many recent days for hourly rollups")
    ap.add_argument("--daily-days", type=int, default=60, help="Refresh this many recent days for daily rollups")
    ap.add_argument("--skip-daily", action="store_true")
    args = ap.parse_args()

    sql = refresh_5m(args.five_minute_hours) + refresh_conversations_5m(args.five_minute_hours) + refresh_1h(args.hourly_days)
    if not args.skip_daily:
        sql += refresh_1d(args.daily_days)
    run_ch(sql)
    result = {
        "status": "ok",
        "refreshed_at": datetime.now(timezone.utc).isoformat(),
        "five_minute_hours": args.five_minute_hours,
        "hourly_days": args.hourly_days,
        "daily_days": None if args.skip_daily else args.daily_days,
        "counts": counts_json(),
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
