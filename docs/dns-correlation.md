# DNS Correlation Readiness

The POC is DNS-ready from a datastore perspective, but live domain attribution requires a DNS telemetry source that emits query/answer events.

## Current state

Current external enrichment includes ASN, organization, country, BGP prefix, and reverse DNS/PTR labels. PTR is useful for readability, but it does **not** prove that a client queried a specific hostname before making a connection.

The DNS-ready datastore adds:

- `flow_poc.dns_events` — normalized DNS query/answer events, one row per client/query/answer tuple.
- `flow_poc.flows_with_dns` — flow view that prefers a recent prior DNS query match for destination labeling, then falls back to PTR, ASN/org, and raw IP.

## Required DNS event fields

Any resolver, firewall, endpoint, or packet-derived DNS source should normalize into these fields:

| Field | Purpose |
|---|---|
| `event_time` | When the DNS answer was observed |
| `client_ip` | Client that issued the query |
| `query_name` | Requested domain/FQDN |
| `query_type` | A, AAAA, CNAME, etc. |
| `response_code` | NOERROR, NXDOMAIN, etc. |
| `answer_ip` | Returned A/AAAA address; one row per answer IP |
| `ttl` | DNS TTL when available |
| `resolver` | Resolver/firewall/sensor that observed the event |
| `source` | Source type, for example `pihole`, `adguard`, `unbound`, `zeek`, `firewall`, `endpoint`, or `synthetic` |

## Correlation model

The first-pass correlation logic joins flows to recent DNS answers by:

```text
flow.src_addr = dns.client_ip
flow.dst_addr = dns.answer_ip
dns.event_time <= flow.flow_start
dns.event_time >= flow.flow_start - 1 hour
```

The view chooses the latest matching DNS answer before the flow using `argMax(query_name, event_time)`.

## Label precedence

Destination display labels should prefer:

```text
DNS query name -> reverse DNS/PTR -> ASN/org -> raw IP
```

This keeps the POC useful now while making the data model ready for true DNS query attribution later.

## Testing without DNS log access

Synthetic testing can validate the datastore and correlation logic without enterprise DNS access:

1. Insert a fake DNS event for a synthetic client/query/answer tuple.
2. Insert or locate a matching flow from the same client to the answer IP after the DNS event time.
3. Query `flow_poc.flows_with_dns` and verify `dst_dns_query_name`, `dst_dns_age_seconds`, and `dst_label_source = 'dns-query'`.

Negative cases to test:

- DNS event occurs after the flow.
- DNS event is older than the correlation window.
- DNS client IP does not match flow source IP.
- DNS answer IP does not match flow destination IP.
- Multiple domains map to the same answer IP; latest prior event should win.

## Caveats

- DNS-over-HTTPS/TLS can bypass resolver logs.
- NAT can reduce confidence if DNS telemetry sees a different client identity than flow telemetry.
- CDN/shared IPs can make domain attribution ambiguous.
- DNS correlation should be treated as high-confidence when same client, same answer IP, and recent prior query all match; PTR-only labels are lower confidence.
