# Operations guide

## Report scripts

Current-flow summary:

```bash
/opt/flow-poc/analyze_flows.py --since 1h --format text
/opt/flow-poc/analyze_flows.py --since "8am" --format text
```

Baseline/anomaly summary:

```bash
/opt/flow-poc/analyze_flow_baselines.py --format text
/opt/flow-poc/analyze_flow_baselines.py --top-n 20 --format text
```

## Dashboard semantics

- Use bits/sec for throughput panels.
- Prefer device names and external organization/ASN names over raw IPs.
- Distinguish observed traffic volume from collector/exporter telemetry overhead.
- Long-lived IPFIX flows may appear bursty if graphed from raw export timestamps.

## Example expected-traffic annotations

Keep environment-specific expected-traffic notes outside the public/template repo. Examples of the type of notes to maintain locally:

```text
client-device-01: corporate proxy/SWG and business SaaS traffic expected
media-device-01: streaming/control traffic expected
mobile-device-01: high-volume QUIC/HTTPS streaming can be expected during active use
admin-workstation-01: administrative SSH or VPN control traffic may be expected when attributed correctly
```

## Useful ClickHouse queries

Recent top internal devices:

```sql
SELECT src_name, src_addr, formatReadableSize(sum(bytes)) AS volume, count() AS flows
FROM flow_poc.flows_enriched_external
WHERE received_at >= now() - INTERVAL 1 HOUR
  AND (startsWith(src_addr,'10.') OR startsWith(src_addr,'192.168.') OR match(src_addr, '^172\\.(1[6-9]|2[0-9]|3[01])\\.'))
GROUP BY src_name, src_addr
ORDER BY sum(bytes) DESC
LIMIT 20;
```

External enrichment coverage:

```sql
SELECT count() AS enriched_ips, uniqExact(asn) AS asns, countIf(reverse_dns != '') AS ptr_records
FROM flow_poc.external_ip_enrichment_current;
```

Rollup freshness:

```sql
SELECT '5m' AS rollup, max(bucket_start) FROM flow_poc.flow_rollup_5m
UNION ALL SELECT '1h', max(bucket_start) FROM flow_poc.flow_rollup_1h
UNION ALL SELECT '1d', max(bucket_start) FROM flow_poc.flow_rollup_1d;
```

## Watch items

- Baseline comparisons need enough historical rollup data to mature.
- Duration-aware 5m rollup is still needed for accurate short-window throughput with long-lived flows.
- External ASN/org enrichment identifies network owner, not always application owner behind cloud IPs.
