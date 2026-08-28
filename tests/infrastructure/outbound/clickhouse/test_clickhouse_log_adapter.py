import logging
import re
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock

from cluster_doctor.infrastructure.outbound.clickhouse.clickhouse_log_adapter import (
    _MAX_ROWS_PER_SEGMENT_PER_SOURCE,
    ClickHouseLogAdapter,
    _split_by_minute,
)
from cluster_doctor.domain.model.log_entry import (
    NodeMetricEntry,
    QueryLogEntry,
    SlowlogEntry,
)
from cluster_doctor.domain.model.time_range import TimeRange

TR       = TimeRange(start=datetime(2026, 8, 20, 2, 9, 0), end=datetime(2026, 8, 20, 2, 10, 0))
TR_MULTI = TimeRange(start=datetime(2026, 8, 20, 2, 9, 30), end=datetime(2026, 8, 20, 2, 11, 15))

# slowlog row 계약: 실제 서버에서 확인한 순서·타입이다.
#   0=발생 시각(_source.`@timestamp`, aware), 1=인덱스명, 2=노드명, 3=took,
#   4=total_hits, 5=total_shards(int), 6=x-opaque-id, 7=쿼리 원문
SLOWLOG_ROW = (
    datetime(2026, 8, 20, 2, 9, 5),
    "lucy_main_v1_20250721",
    "RC17-10",
    "32.4s",
    "68 hits",
    902,
    "service=web,project=quettai,env=prod,company=50,user=579,action=count",
    '{"size":0,"query":{"query_string":{"query":"생선"}}}',
)


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
    client = _make_client(slowlog_rows=[SLOWLOG_ROW])
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")
    sl      = [l for l in adapter.fetch_logs(TR) if l.source == "slowlog"]

    assert len(sl) == 1
    e = sl[0]
    assert isinstance(e, SlowlogEntry)
    assert e.timestamp    == datetime(2026, 8, 20, 2, 9, 5)
    assert e.index_name   == "lucy_main_v1_20250721"
    assert e.node         == "RC17-10"
    assert e.took         == "32.4s"       # 느린 정도
    assert e.total_hits   == "68 hits"     # 결과량
    assert e.total_shards == 902           # 조회된 샤드 수 — int로 남는다
    assert "company=50"   in e.opaque_id   # company/user 귀속
    assert "생선"          in e.query       # 쿼리 원문


def test_slowlog_projects_named_subcolumns_instead_of_the_whole_source():
    # _source를 통째로 가져오면 두 가지가 동시에 깨진다.
    #  1) clickhouse-connect는 JSON 타입을 dict로 돌려준다 -> LogEntry.message의
    #     str 계약이 깨지고 프롬프트에 dict repr이 실린다.
    #  2) host.mac/agent.ephemeral_id/host.os.kernel 같은 진단과 무관한 필드가
    #     행당 2.7KB 중 73%를 차지한다(실측).
    # 테스트 더블은 어느 쪽도 재현하지 못하므로 SQL의 투영을 직접 검증한다.
    client  = _make_client()
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")
    adapter.fetch_logs(TR)

    sql = next(
        c.args[0] for c in client.query.call_args_list if "slowlog_v2" in c.args[0].lower()
    )
    select = sql.lower().split("from", 1)[0]
    assert "_source.elasticsearch.slowlog.took" in select
    assert "_source.elasticsearch.index.name" in select
    # 통째 투영(SELECT ... _source, ... / SELECT _source FROM)이 남아 있으면 안 된다.
    assert not re.search(r"[\s,]_source\s*(,|$)", select), select


def test_slowlog_is_filtered_by_occurrence_time_not_ingestion_time():
    # ch_ingested_at은 ClickHouse 적재 시각이다. 실측 지연이 23~41초라
    # 분 경계를 넘기면 트리거를 유발한 그 slowlog가 조회 구간에서 빠지고,
    # 분 단위 버킷의 시각 라벨도 통째로 밀린다.
    client  = _make_client()
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")
    adapter.fetch_logs(TR)

    sql = next(
        c.args[0] for c in client.query.call_args_list if "slowlog_v2" in c.args[0].lower()
    )
    where = sql.lower().split("where", 1)[1]
    assert "@timestamp" in where
    assert "ch_ingested_at" not in where


QUERY_ROW = (
    datetime(2026, 8, 20, 2, 9, 10), "host1", Decimal("0.5"), "Y", "GET",
    "svc", "prod", "proj", "cls1", ["kwd", "kwd2"], "acme", "alice",
)


def test_fetch_logs_maps_query_log_success():
    client = _make_client(query_rows=[QUERY_ROW])
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")
    ql      = [l for l in adapter.fetch_logs(TR) if l.source == "es_query_log"]

    assert len(ql) == 1
    e = ql[0]
    assert isinstance(e, QueryLogEntry)
    assert e.success  is True
    assert e.service  == "svc"
    assert e.host     == "host1"
    assert e.cmd      == "GET"
    assert e.project  == "proj"
    assert e.run_time == Decimal("0.5")     # Decimal 그대로. 문자열이 아니다
    assert e.company  == "acme"
    assert e.user     == "alice"


def test_query_log_keywords_become_a_hashable_tuple():
    # ClickHouse는 list를 준다. frozen dataclass에서 list 필드는 해시를 깨뜨린다.
    client = _make_client(query_rows=[QUERY_ROW])
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")
    e = [l for l in adapter.fetch_logs(TR) if l.source == "es_query_log"][0]

    assert e.keywords == ("kwd", "kwd2")
    assert hash(e) is not None


def test_fetch_logs_maps_query_log_fail():
    row    = (*QUERY_ROW[:3], "N", *QUERY_ROW[4:])
    client = _make_client(query_rows=[row])
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")
    ql      = [l for l in adapter.fetch_logs(TR) if l.source == "es_query_log"]
    assert ql[0].success is False


def test_fetch_logs_maps_node_metric():
    row    = (datetime(2026, 8, 20, 2, 9, 0), "node1", "10.0.0.1", 30, 60, 15, 70, 2, 0, 0, 1, 0, 0)
    client = _make_client(metric_rows=[row])
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")
    metrics = [l for l in adapter.fetch_logs(TR) if l.source == "node_metric"]

    assert len(metrics) == 1
    e = metrics[0]
    assert isinstance(e, NodeMetricEntry)
    assert e.node_name             == "node1"
    assert e.node_ip               == "10.0.0.1"
    assert e.os_cpu_percent        == 30      # int로 남아 임계치 비교가 가능하다
    assert e.jvm_heap_used_percent == 70
    assert e.search_active         == 2
    assert e.write_active          == 1


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


def _slowlog_rows(count):
    return [SLOWLOG_ROW] * count


def test_warns_when_a_segment_query_returns_exactly_the_limit(caplog):
    # The LIMIT has no ORDER BY, so hitting the cap means ClickHouse dropped
    # an arbitrary subset with no signal in the result -- and _build_prompt
    # then reports the capped count to the model as if it were the total.
    client  = _make_client(slowlog_rows=_slowlog_rows(_MAX_ROWS_PER_SEGMENT_PER_SOURCE))
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")

    with caplog.at_level(logging.WARNING):
        adapter.fetch_logs(TR)

    truncation_warnings = [
        r for r in caplog.records
        if r.levelno == logging.WARNING and "LIMIT" in r.getMessage()
    ]
    assert len(truncation_warnings) == 1, [r.getMessage() for r in caplog.records]
    message = truncation_warnings[0].getMessage()
    # Operators need to know *which* source and *which* segment truncated;
    # a bare "truncation happened" line is not actionable.
    assert "slowlog" in message
    assert str(_MAX_ROWS_PER_SEGMENT_PER_SOURCE) in message
    assert TR.start.isoformat() in message
    assert TR.end.isoformat() in message


def test_does_not_warn_when_a_segment_query_stays_below_the_limit(caplog):
    client  = _make_client(slowlog_rows=_slowlog_rows(_MAX_ROWS_PER_SEGMENT_PER_SOURCE - 1))
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")

    with caplog.at_level(logging.WARNING):
        adapter.fetch_logs(TR)

    assert [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING] == []


def test_warns_once_per_truncated_segment_and_source(caplog):
    # TR_MULTI spans three one-minute segments; every slowlog segment
    # truncates, so each must be reported separately -- one aggregate
    # warning would hide which minute of the window is affected.
    client  = _make_client(slowlog_rows=_slowlog_rows(_MAX_ROWS_PER_SEGMENT_PER_SOURCE))
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")

    with caplog.at_level(logging.WARNING):
        adapter.fetch_logs(TR_MULTI)

    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 3, warnings
    assert len(set(warnings)) == 3, "each warning must name its own segment"


def test_fetch_logs_sorted_descending():
    t2     = datetime(2026, 8, 20, 2, 9, 30)
    client = _make_client(
        slowlog_rows=[SLOWLOG_ROW],
        query_rows=[(t2, "h", 0.1, "Y", "GET", "s", "e", "p", "c", "k", None, None)],
    )
    adapter = ClickHouseLogAdapter(client, "slowlog_v2", "log", "es_node_metric")
    logs    = adapter.fetch_logs(TR)
    times   = [l.timestamp for l in logs]
    assert times == sorted(times, reverse=True)
