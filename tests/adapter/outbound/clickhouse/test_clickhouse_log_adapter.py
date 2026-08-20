from datetime import datetime
from unittest.mock import MagicMock

from cluster_doctor.adapter.outbound.clickhouse.clickhouse_log_adapter import (
    _MAX_ROWS_PER_SEGMENT_PER_SOURCE,
    ClickHouseLogAdapter,
    _split_by_minute,
)
from cluster_doctor.domain.model.time_range import TimeRange

TR       = TimeRange(start=datetime(2026, 8, 20, 2, 9, 0), end=datetime(2026, 8, 20, 2, 10, 0))
TR_MULTI = TimeRange(start=datetime(2026, 8, 20, 2, 9, 30), end=datetime(2026, 8, 20, 2, 11, 15))


def _make_client(slowlog_rows=None, query_rows=None, metric_rows=None):
    client = MagicMock()

    def side_effect(query, parameters=None):
        result = MagicMock()
        q = query.lower()
        if "from slowlog_v2" in q:
            result.result_rows = slowlog_rows or []
        elif "from es_node_metric" in q:
            result.result_rows = metric_rows or []
        elif "from log " in q:
            result.result_rows = query_rows or []
        else:
            raise AssertionError(f"unrecognized query in test double: {query!r}")
        return result

    client.query.side_effect = side_effect
    return client


def test_split_exact_one_minute():
    segs = _split_by_minute(TR)
    assert len(segs) == 1
    assert segs[0].start == datetime(2026, 8, 20, 2, 9, 0)
    assert segs[0].end   == datetime(2026, 8, 20, 2, 10, 0)


def test_split_crosses_two_boundaries():
    segs = _split_by_minute(TR_MULTI)
    assert len(segs) == 3
    assert segs[0].start == datetime(2026, 8, 20, 2, 9, 30)
    assert segs[0].end   == datetime(2026, 8, 20, 2, 10, 0)
    assert segs[1].start == datetime(2026, 8, 20, 2, 10, 0)
    assert segs[1].end   == datetime(2026, 8, 20, 2, 11, 0)
    assert segs[2].start == datetime(2026, 8, 20, 2, 11, 0)
    assert segs[2].end   == datetime(2026, 8, 20, 2, 11, 15)


def test_fetch_logs_maps_slowlog():
    client = _make_client(slowlog_rows=[(datetime(2026, 8, 20, 2, 9, 5), '{"took":100}')])
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")
    logs    = adapter.fetch_logs(TR)
    sl      = [l for l in logs if l.source == "slowlog"]
    assert len(sl) == 1
    assert sl[0].level   == "SLOWLOG"
    assert sl[0].message == '{"took":100}'
    assert sl[0].component is None
    assert sl[0].node is None


def test_fetch_logs_maps_query_log_success():
    row    = (datetime(2026, 8, 20, 2, 9, 10), "host1", 0.5, "Y", "GET", "svc", "prod", "proj", "cls1", 10, "kwd")
    client = _make_client(query_rows=[row])
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")
    logs    = adapter.fetch_logs(TR)
    ql      = [l for l in logs if l.source == "es_query_log"]
    assert len(ql) == 1
    assert ql[0].level     == "SUCCESS"
    assert ql[0].component == "svc"
    assert ql[0].node      == "host1"
    assert "[GET]" in ql[0].message
    assert "proj" in ql[0].message
    assert "runtime=0.5s" in ql[0].message
    assert "keyword=kwd" in ql[0].message


def test_fetch_logs_maps_query_log_fail():
    row    = (datetime(2026, 8, 20, 2, 9, 10), "host1", 1.2, "N", "POST", "svc", "prod", "proj", "cls1", 0, "")
    client = _make_client(query_rows=[row])
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")
    logs    = adapter.fetch_logs(TR)
    ql      = [l for l in logs if l.source == "es_query_log"]
    assert ql[0].level == "FAIL"


def test_fetch_logs_maps_node_metric():
    row    = (datetime(2026, 8, 20, 2, 9, 0), "node1", "10.0.0.1", 30.5, 60.0, 15.0, 70.0, 2, 0, 0, 1, 0, 0)
    client = _make_client(metric_rows=[row])
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")
    logs    = adapter.fetch_logs(TR)
    metrics = [l for l in logs if l.source == "node_metric"]
    assert len(metrics) == 1
    assert metrics[0].level   == "METRIC"
    assert metrics[0].node    == "node1 (10.0.0.1)"
    assert "cpu=30.5%"        in metrics[0].message
    assert "jvm_heap=70.0%"   in metrics[0].message
    assert "search(active=2," in metrics[0].message
    assert "write(active=1,"  in metrics[0].message


def test_fetch_logs_queries_each_source_per_minute_segment():
    client  = _make_client()
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")
    adapter.fetch_logs(TR_MULTI)
    assert client.query.call_count == 9


def test_fetch_logs_slowlog_query_has_limit():
    client  = _make_client()
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")
    adapter.fetch_logs(TR)
    slowlog_calls = [
        call for call in client.query.call_args_list if "slowlog_v2" in call.args[0].lower()
    ]
    assert slowlog_calls
    for call in slowlog_calls:
        assert f"LIMIT {_MAX_ROWS_PER_SEGMENT_PER_SOURCE}" in call.args[0]


def test_fetch_logs_query_log_query_has_limit():
    client  = _make_client()
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")
    adapter.fetch_logs(TR)
    log_calls = [
        call for call in client.query.call_args_list if "from log " in call.args[0].lower()
    ]
    assert log_calls
    for call in log_calls:
        assert f"LIMIT {_MAX_ROWS_PER_SEGMENT_PER_SOURCE}" in call.args[0]


def test_fetch_logs_node_metric_query_has_limit():
    client  = _make_client()
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")
    adapter.fetch_logs(TR)
    metric_calls = [
        call for call in client.query.call_args_list if "es_node_metric" in call.args[0].lower()
    ]
    assert metric_calls
    for call in metric_calls:
        assert f"LIMIT {_MAX_ROWS_PER_SEGMENT_PER_SOURCE}" in call.args[0]


def test_fetch_logs_sorted_descending():
    t1     = datetime(2026, 8, 20, 2, 9, 1)
    t2     = datetime(2026, 8, 20, 2, 9, 30)
    client = _make_client(
        slowlog_rows=[(t1, "slow")],
        query_rows=[(t2, "h", 0.1, "Y", "GET", "s", "e", "p", "c", 1, "k")],
    )
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")
    logs    = adapter.fetch_logs(TR)
    times   = [l.timestamp for l in logs]
    assert times == sorted(times, reverse=True)
