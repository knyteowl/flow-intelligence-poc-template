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

CREATE TABLE IF NOT EXISTS flow_poc.flow_conversation_5m
(
    bucket_start DateTime,
    endpoint_a_addr String,
    endpoint_a_name String,
    endpoint_a_device_type String,
    endpoint_b_addr String,
    endpoint_b_name String,
    endpoint_b_device_type String,
    proto String,
    service_port UInt16,
    bytes UInt64,
    packets UInt64,
    flows UInt64,
    first_seen DateTime64(9),
    last_seen DateTime64(9),
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (bucket_start, endpoint_a_addr, endpoint_b_addr, proto, service_port)
TTL bucket_start + INTERVAL 30 DAY DELETE;

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

CREATE OR REPLACE VIEW flow_poc.v_conversation_5m_bidirectional AS
SELECT
    bucket_start,
    endpoint_a_addr,
    any(endpoint_a_name) AS endpoint_a_name,
    any(endpoint_a_device_type) AS endpoint_a_device_type,
    endpoint_b_addr,
    any(endpoint_b_name) AS endpoint_b_name,
    any(endpoint_b_device_type) AS endpoint_b_device_type,
    proto,
    service_port,
    sum(bytes) AS bytes,
    sum(packets) AS packets,
    sum(flows) AS flows,
    min(first_seen) AS first_seen,
    max(last_seen) AS last_seen
FROM flow_poc.flow_conversation_5m FINAL
GROUP BY bucket_start, endpoint_a_addr, endpoint_b_addr, proto, service_port;
