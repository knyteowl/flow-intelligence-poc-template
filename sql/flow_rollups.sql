CREATE TABLE IF NOT EXISTS flow_poc.flow_rollup_5m
(
    bucket_start DateTime,
    src_addr String,
    src_name String,
    src_device_type String,
    dst_addr String,
    dst_name String,
    dst_device_type String,
    proto String,
    dst_port UInt16,
    bytes UInt64,
    packets UInt64,
    flows UInt64,
    first_seen DateTime64(9),
    last_seen DateTime64(9),
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (bucket_start, src_addr, dst_addr, proto, dst_port)
TTL bucket_start + INTERVAL 30 DAY DELETE;

CREATE TABLE IF NOT EXISTS flow_poc.flow_rollup_1h
(
    bucket_start DateTime,
    src_addr String,
    src_name String,
    src_device_type String,
    dst_addr String,
    dst_name String,
    dst_device_type String,
    proto String,
    dst_port UInt16,
    bytes UInt64,
    packets UInt64,
    flows UInt64,
    first_seen DateTime64(9),
    last_seen DateTime64(9),
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (bucket_start, src_addr, dst_addr, proto, dst_port)
TTL bucket_start + INTERVAL 180 DAY DELETE;

CREATE TABLE IF NOT EXISTS flow_poc.flow_rollup_1d
(
    bucket_start Date,
    src_addr String,
    src_name String,
    src_device_type String,
    dst_addr String,
    dst_name String,
    dst_device_type String,
    proto String,
    dst_port UInt16,
    bytes UInt64,
    packets UInt64,
    flows UInt64,
    first_seen DateTime64(9),
    last_seen DateTime64(9),
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (bucket_start, src_addr, dst_addr, proto, dst_port)
TTL bucket_start + INTERVAL 365 DAY DELETE;

CREATE OR REPLACE VIEW flow_poc.v_rollup_device_5m AS
SELECT
    bucket_start,
    src_addr,
    any(src_name) AS src_name,
    any(src_device_type) AS src_device_type,
    sum(bytes) AS bytes,
    sum(packets) AS packets,
    sum(flows) AS flows
FROM flow_poc.flow_rollup_5m FINAL
GROUP BY bucket_start, src_addr;

CREATE OR REPLACE VIEW flow_poc.v_rollup_device_1h AS
SELECT
    bucket_start,
    src_addr,
    any(src_name) AS src_name,
    any(src_device_type) AS src_device_type,
    sum(bytes) AS bytes,
    sum(packets) AS packets,
    sum(flows) AS flows
FROM flow_poc.flow_rollup_1h FINAL
GROUP BY bucket_start, src_addr;

CREATE OR REPLACE VIEW flow_poc.v_rollup_device_1d AS
SELECT
    bucket_start,
    src_addr,
    any(src_name) AS src_name,
    any(src_device_type) AS src_device_type,
    sum(bytes) AS bytes,
    sum(packets) AS packets,
    sum(flows) AS flows
FROM flow_poc.flow_rollup_1d FINAL
GROUP BY bucket_start, src_addr;
