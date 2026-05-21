CREATE DATABASE IF NOT EXISTS flow_poc;

CREATE TABLE IF NOT EXISTS flow_poc.flows
(
    received_at DateTime64(9),
    flow_start DateTime64(9),
    flow_end DateTime64(9),
    sampler_address String,
    src_addr String,
    dst_addr String,
    proto String,
    src_port UInt16,
    dst_port UInt16,
    bytes UInt64,
    packets UInt64,
    in_if UInt32,
    out_if UInt32,
    tcp_flags UInt32,
    next_hop String
)
ENGINE = MergeTree
PARTITION BY toDate(received_at)
ORDER BY (received_at, src_addr, dst_addr, dst_port)
TTL toDateTime(received_at) + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS flow_poc.flows_kafka
(
    time_received_ns UInt64,
    time_flow_start_ns UInt64,
    time_flow_end_ns UInt64,
    sampler_address String,
    src_addr String,
    dst_addr String,
    proto String,
    src_port UInt16,
    dst_port UInt16,
    bytes UInt64,
    packets UInt64,
    in_if UInt32,
    out_if UInt32,
    tcp_flags UInt32,
    next_hop String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'flows.raw',
    kafka_group_name = 'clickhouse-flow-consumer-v2',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream',
    input_format_skip_unknown_fields = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS flow_poc.flows_mv TO flow_poc.flows AS
SELECT
    fromUnixTimestamp64Nano(toInt64(time_received_ns)) AS received_at,
    fromUnixTimestamp64Nano(toInt64(time_flow_start_ns)) AS flow_start,
    fromUnixTimestamp64Nano(toInt64(time_flow_end_ns)) AS flow_end,
    sampler_address,
    src_addr,
    dst_addr,
    proto,
    src_port,
    dst_port,
    bytes,
    packets,
    in_if,
    out_if,
    tcp_flags,
    next_hop
FROM flow_poc.flows_kafka;

CREATE TABLE IF NOT EXISTS flow_poc.device_inventory
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

CREATE OR REPLACE VIEW flow_poc.device_inventory_current AS
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
    max(updated_at) AS updated_at
FROM flow_poc.device_inventory
GROUP BY ip;

CREATE OR REPLACE VIEW flow_poc.flows_enriched AS
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
FROM flow_poc.flows AS f
LEFT JOIN flow_poc.device_inventory_current AS src ON f.src_addr = src.ip
LEFT JOIN flow_poc.device_inventory_current AS dst ON f.dst_addr = dst.ip;

CREATE OR REPLACE VIEW flow_poc.v_top_talkers_15m AS
SELECT src_name, src_addr, sum(bytes) AS bytes, sum(packets) AS packets, count() AS flows
FROM flow_poc.flows_enriched
WHERE received_at > now() - INTERVAL 15 MINUTE
GROUP BY src_name, src_addr
ORDER BY bytes DESC;

CREATE OR REPLACE VIEW flow_poc.v_top_destinations_15m AS
SELECT dst_name, dst_addr, sum(bytes) AS bytes, sum(packets) AS packets, count() AS flows
FROM flow_poc.flows_enriched
WHERE received_at > now() - INTERVAL 15 MINUTE
GROUP BY dst_name, dst_addr
ORDER BY bytes DESC;
