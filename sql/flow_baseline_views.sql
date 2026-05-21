CREATE OR REPLACE VIEW flow_poc.v_device_hourly_baseline AS
SELECT
    src_addr,
    any(src_name) AS src_name,
    any(src_device_type) AS src_device_type,
    toHour(bucket_start) AS hour_of_day,
    count() AS buckets,
    avg(bytes) AS avg_bytes,
    stddevPop(bytes) AS stddev_bytes,
    quantile(0.50)(bytes) AS p50_bytes,
    quantile(0.95)(bytes) AS p95_bytes,
    max(bytes) AS max_bytes,
    avg(flows) AS avg_flows,
    quantile(0.95)(flows) AS p95_flows
FROM flow_poc.v_rollup_device_1h
WHERE bucket_start < toStartOfHour(now())
  AND (startsWith(src_addr,'10.') OR startsWith(src_addr,'192.168.') OR match(src_addr, '^172\\.(1[6-9]|2[0-9]|3[01])\\.'))
GROUP BY src_addr, hour_of_day;

CREATE OR REPLACE VIEW flow_poc.v_device_daily_baseline AS
SELECT
    src_addr,
    any(src_name) AS src_name,
    any(src_device_type) AS src_device_type,
    count() AS days,
    avg(bytes) AS avg_bytes,
    stddevPop(bytes) AS stddev_bytes,
    quantile(0.50)(bytes) AS p50_bytes,
    quantile(0.95)(bytes) AS p95_bytes,
    max(bytes) AS max_bytes,
    avg(flows) AS avg_flows,
    quantile(0.95)(flows) AS p95_flows
FROM flow_poc.v_rollup_device_1d
WHERE bucket_start < today()
  AND (startsWith(src_addr,'10.') OR startsWith(src_addr,'192.168.') OR match(src_addr, '^172\\.(1[6-9]|2[0-9]|3[01])\\.'))
GROUP BY src_addr;

CREATE OR REPLACE VIEW flow_poc.v_current_hour_device_vs_baseline AS
WITH toStartOfHour(now()) AS current_hour
SELECT
    c.bucket_start,
    c.src_addr,
    c.src_name,
    c.src_device_type,
    c.bytes,
    c.packets,
    c.flows,
    b.buckets AS baseline_buckets,
    b.avg_bytes,
    b.p95_bytes,
    b.max_bytes,
    if(b.stddev_bytes > 0, (c.bytes - b.avg_bytes) / b.stddev_bytes, NULL) AS z_bytes,
    if(b.avg_bytes > 0, c.bytes / b.avg_bytes, NULL) AS ratio_to_avg,
    if(b.p95_bytes > 0, c.bytes / b.p95_bytes, NULL) AS ratio_to_p95
FROM flow_poc.v_rollup_device_1h AS c
LEFT JOIN flow_poc.v_device_hourly_baseline AS b
    ON c.src_addr = b.src_addr AND toHour(c.bucket_start) = b.hour_of_day
WHERE c.bucket_start = current_hour
  AND (startsWith(c.src_addr,'10.') OR startsWith(c.src_addr,'192.168.') OR match(c.src_addr, '^172\\.(1[6-9]|2[0-9]|3[01])\\.'));

CREATE OR REPLACE VIEW flow_poc.v_recent_new_dst_ports AS
SELECT
    recent.src_addr,
    any(recent.src_name) AS src_name,
    recent.proto,
    recent.dst_port,
    sum(recent.bytes) AS recent_bytes,
    sum(recent.flows) AS recent_flows
FROM flow_poc.flow_rollup_5m AS recent
LEFT ANTI JOIN
(
    SELECT DISTINCT src_addr, proto, dst_port
    FROM flow_poc.flow_rollup_1h
    WHERE bucket_start >= now() - INTERVAL 14 DAY
      AND bucket_start < now() - INTERVAL 1 HOUR
) AS prior
ON recent.src_addr = prior.src_addr AND recent.proto = prior.proto AND recent.dst_port = prior.dst_port
WHERE recent.bucket_start >= now() - INTERVAL 1 HOUR
  AND (startsWith(recent.src_addr,'10.') OR startsWith(recent.src_addr,'192.168.') OR match(recent.src_addr, '^172\\.(1[6-9]|2[0-9]|3[01])\\.'))
GROUP BY recent.src_addr, recent.proto, recent.dst_port
ORDER BY recent_bytes DESC;

CREATE OR REPLACE VIEW flow_poc.v_recent_new_external_destinations AS
SELECT
    recent.src_addr,
    any(recent.src_name) AS src_name,
    recent.dst_addr,
    any(recent.dst_name) AS dst_name,
    recent.proto,
    recent.dst_port,
    sum(recent.bytes) AS recent_bytes,
    sum(recent.flows) AS recent_flows
FROM flow_poc.flow_rollup_5m AS recent
LEFT ANTI JOIN
(
    SELECT DISTINCT src_addr, dst_addr, proto, dst_port
    FROM flow_poc.flow_rollup_1h
    WHERE bucket_start >= now() - INTERVAL 14 DAY
      AND bucket_start < now() - INTERVAL 1 HOUR
) AS prior
ON recent.src_addr = prior.src_addr AND recent.dst_addr = prior.dst_addr AND recent.proto = prior.proto AND recent.dst_port = prior.dst_port
WHERE recent.bucket_start >= now() - INTERVAL 1 HOUR
  AND (startsWith(recent.src_addr,'10.') OR startsWith(recent.src_addr,'192.168.') OR match(recent.src_addr, '^172\\.(1[6-9]|2[0-9]|3[01])\\.'))
  AND NOT (startsWith(recent.dst_addr,'10.') OR startsWith(recent.dst_addr,'192.168.') OR match(recent.dst_addr, '^172\\.(1[6-9]|2[0-9]|3[01])\\.'))
GROUP BY recent.src_addr, recent.dst_addr, recent.proto, recent.dst_port
ORDER BY recent_bytes DESC;
