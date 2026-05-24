-- DNS correlation datastore readiness.
--
-- This schema is intentionally source-neutral: resolver logs, firewall DNS logs,
-- endpoint telemetry, Zeek DNS logs, or synthetic test data can all be normalized
-- into one row per client/query/answer tuple.
--
-- Reverse DNS/PTR enrichment is useful labeling, but it is not proof that a
-- client resolved a name before making a connection. True DNS correlation needs
-- DNS query/answer telemetry with event_time, client_ip, query_name, answer_ip,
-- and TTL when available.

CREATE TABLE IF NOT EXISTS flow_poc.dns_events
(
    event_time DateTime64(3),
    client_ip String,
    query_name String,
    normalized_query_name String DEFAULT lower(trim(BOTH '.' FROM query_name)),
    query_type LowCardinality(String),
    response_code LowCardinality(String),
    answer_ip String,
    answer_type LowCardinality(String) DEFAULT query_type,
    ttl UInt32,
    resolver String,
    source LowCardinality(String),
    ingest_batch_id String DEFAULT '',
    raw_event String DEFAULT '',
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (client_ip, answer_ip, event_time, normalized_query_name)
TTL toDateTime(event_time) + INTERVAL 30 DAY DELETE;

CREATE OR REPLACE VIEW flow_poc.flows_with_dns AS
SELECT
    f.received_at,
    f.flow_start,
    f.flow_end,
    f.sampler_address,
    f.src_addr,
    f.dst_addr,
    f.proto,
    f.src_port,
    f.dst_port,
    f.bytes,
    f.packets,
    f.in_if,
    f.out_if,
    f.tcp_flags,
    f.next_hop,
    f.src_name,
    f.src_mac,
    f.src_vendor,
    f.src_device_type,
    f.src_network,
    f.dst_name,
    f.dst_mac,
    f.dst_vendor,
    f.dst_device_type,
    f.dst_network,
    f.src_asn,
    f.src_as_name,
    f.src_bgp_prefix,
    f.src_country_code,
    f.src_reverse_dns,
    f.dst_asn,
    f.dst_as_name,
    f.dst_bgp_prefix,
    f.dst_country_code,
    f.dst_reverse_dns,
    f.src_org_or_name,
    f.dst_org_or_name,
    if(d.event_time >= f.flow_start - INTERVAL 1 HOUR, d.query_name, '') AS dst_dns_query_name,
    if(d.event_time >= f.flow_start - INTERVAL 1 HOUR, d.normalized_query_name, '') AS dst_dns_normalized_query_name,
    if(d.event_time >= f.flow_start - INTERVAL 1 HOUR, d.event_time, NULL) AS dst_dns_event_time,
    if(d.event_time >= f.flow_start - INTERVAL 1 HOUR, dateDiff('second', d.event_time, f.flow_start), NULL) AS dst_dns_age_seconds,
    if(d.event_time >= f.flow_start - INTERVAL 1 HOUR, d.source, '') AS dst_dns_source,
    if(d.event_time >= f.flow_start - INTERVAL 1 HOUR, d.resolver, '') AS dst_dns_resolver,
    if(dst_dns_query_name != '', 'dns-query', if(f.dst_reverse_dns != '', 'ptr', if(f.dst_as_name != '', 'asn-org', 'raw-ip'))) AS dst_label_source,
    coalesce(nullIf(dst_dns_query_name, ''), nullIf(f.dst_reverse_dns, ''), nullIf(f.dst_org_or_name, ''), f.dst_addr) AS dst_dns_or_org_name
FROM flow_poc.flows_enriched_external AS f
ASOF LEFT JOIN
(
    SELECT
        client_ip,
        answer_ip,
        event_time,
        query_name,
        normalized_query_name,
        source,
        resolver
    FROM flow_poc.dns_events
    WHERE answer_ip != ''
    ORDER BY client_ip, answer_ip, event_time
) AS d
    ON f.src_addr = d.client_ip
   AND f.dst_addr = d.answer_ip
   AND f.flow_start >= d.event_time;
