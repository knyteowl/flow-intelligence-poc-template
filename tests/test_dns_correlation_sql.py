from pathlib import Path


SQL_PATH = Path(__file__).resolve().parents[1] / "sql" / "dns_correlation.sql"


def normalized_sql() -> str:
    return " ".join(SQL_PATH.read_text().split())


def test_dns_events_table_is_ready_for_query_answer_telemetry():
    sql = normalized_sql()

    assert "CREATE TABLE IF NOT EXISTS flow_poc.dns_events" in sql
    assert "event_time DateTime64(3)" in sql
    assert "client_ip String" in sql
    assert "query_name String" in sql
    assert "normalized_query_name String" in sql
    assert "query_type LowCardinality(String)" in sql
    assert "response_code LowCardinality(String)" in sql
    assert "answer_ip String" in sql
    assert "ttl UInt32" in sql
    assert "resolver String" in sql
    assert "source LowCardinality(String)" in sql
    assert "ORDER BY (client_ip, answer_ip, event_time, normalized_query_name)" in sql
    assert "TTL toDateTime(event_time) + INTERVAL 30 DAY DELETE" in sql


def test_dns_correlation_view_matches_flows_to_latest_prior_dns_answer():
    sql = normalized_sql()

    assert "CREATE OR REPLACE VIEW flow_poc.flows_with_dns" in sql
    assert "FROM flow_poc.flows_enriched_external AS f" in sql
    assert "ASOF LEFT JOIN" in sql
    assert "flow_poc.dns_events" in sql
    assert "f.src_addr = d.client_ip" in sql
    assert "f.dst_addr = d.answer_ip" in sql
    assert "f.flow_start >= d.event_time" in sql
    assert "d.event_time >= f.flow_start - INTERVAL 1 HOUR" in sql
    assert "if(d.event_time >= f.flow_start - INTERVAL 1 HOUR, d.query_name, '') AS dst_dns_query_name" in sql
    assert "dateDiff('second', d.event_time, f.flow_start)" in sql
    assert "AS dst_dns_age_seconds" in sql
    assert "if(dst_dns_query_name != '', 'dns-query'," in sql
