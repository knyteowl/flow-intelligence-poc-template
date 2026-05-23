import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "refresh_flow_rollups.py"
spec = importlib.util.spec_from_file_location("refresh_flow_rollups", MODULE_PATH)
rollups = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rollups)


def normalized_sql(sql: str) -> str:
    return " ".join(sql.split())


def test_refresh_5m_distributes_long_lived_flows_across_touched_buckets():
    sql = normalized_sql(rollups.refresh_5m(6))

    assert "arrayJoin(range(" in sql
    assert "flow_start" in sql
    assert "flow_end" in sql
    assert "overlap_seconds" in sql
    assert "duration_seconds" in sql
    assert "toStartOfInterval(flow_start_norm, INTERVAL 5 MINUTE)" in sql
    assert "toUInt64(round(bytes * overlap_seconds / duration_seconds))" in sql
    assert "received_at >= now() - INTERVAL 6 HOUR" in sql


def test_refresh_conversations_5m_builds_bidirectional_endpoint_pairs():
    sql = normalized_sql(rollups.refresh_conversations_5m(6))

    assert "flow_conversation_5m" in sql
    assert "endpoint_a_addr" in sql
    assert "endpoint_b_addr" in sql
    assert "if(src_addr <= dst_addr, src_addr, dst_addr)" in sql
    assert "if(src_addr <= dst_addr, dst_addr, src_addr)" in sql
    assert "service_port" in sql
    assert "least(src_port, dst_port)" in sql
    assert "arrayJoin(range(" in sql
    assert "overlap_seconds" in sql
