CREATE TABLE IF NOT EXISTS flow_poc.external_ip_enrichment
(
    ip String,
    asn UInt32,
    as_name String,
    bgp_prefix String,
    country_code String,
    registry String,
    allocated String,
    reverse_dns String,
    source String,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY ip
TTL updated_at + INTERVAL 30 DAY;

CREATE OR REPLACE VIEW flow_poc.external_ip_enrichment_current AS
SELECT
    ip,
    argMax(asn, updated_at) AS asn,
    argMax(as_name, updated_at) AS as_name,
    argMax(bgp_prefix, updated_at) AS bgp_prefix,
    argMax(country_code, updated_at) AS country_code,
    argMax(registry, updated_at) AS registry,
    argMax(allocated, updated_at) AS allocated,
    argMax(reverse_dns, updated_at) AS reverse_dns,
    argMax(source, updated_at) AS source,
    max(updated_at) AS latest_updated_at
FROM flow_poc.external_ip_enrichment
GROUP BY ip;

CREATE OR REPLACE VIEW flow_poc.flows_enriched_external AS
SELECT
    f.*,
    src_ext.asn AS src_asn,
    src_ext.as_name AS src_as_name,
    src_ext.bgp_prefix AS src_bgp_prefix,
    src_ext.country_code AS src_country_code,
    src_ext.reverse_dns AS src_reverse_dns,
    dst_ext.asn AS dst_asn,
    dst_ext.as_name AS dst_as_name,
    dst_ext.bgp_prefix AS dst_bgp_prefix,
    dst_ext.country_code AS dst_country_code,
    dst_ext.reverse_dns AS dst_reverse_dns,
    if(dst_ext.as_name != '', dst_ext.as_name, f.dst_name) AS dst_org_or_name,
    if(src_ext.as_name != '', src_ext.as_name, f.src_name) AS src_org_or_name
FROM flow_poc.flows_enriched AS f
LEFT JOIN flow_poc.external_ip_enrichment_current AS src_ext ON f.src_addr = src_ext.ip
LEFT JOIN flow_poc.external_ip_enrichment_current AS dst_ext ON f.dst_addr = dst_ext.ip;
