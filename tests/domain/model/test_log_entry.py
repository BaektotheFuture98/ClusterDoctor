"""소스별 로그 항목.

하나의 LogEntry로 셋을 합쳐 쓰던 것을 소스마다 하나씩으로 나눈다. 합쳐 쓰는
동안 두 가지가 무너져 있었다.

  - 같은 필드가 소스마다 다른 뜻이었다. ``component``가 slowlog에서는
    인덱스명, es_query_log에서는 service, node_metric에서는 None이었다.
  - 값이 ``message`` 문자열로 평탄화됐다. run_time·keyword·cpu가 전부 텍스트가
    되어, 임계치로 거르거나 keyword를 자르려면 문자열을 다시 파싱해야 했다.
"""

from dataclasses import FrozenInstanceError
from datetime import datetime
from decimal import Decimal

import pytest

from cluster_doctor.domain.model.log_entry import (
    LogEntry,
    NodeMetricEntry,
    QueryLogEntry,
    SlowlogEntry,
)

TS = datetime(2026, 8, 27, 18, 33, 2)


def _slowlog(**kw) -> SlowlogEntry:
    return SlowlogEntry(
        timestamp=TS, index_name="app_index_v1", node="node-a01", took="32.4s",
        total_hits="68 hits", total_shards=902,
        opaque_id="service=web,company=1,user=2", query='{"size":0}', **kw
    )


def _querylog(**kw) -> QueryLogEntry:
    base = dict(
        timestamp=TS, host="10.0.0.11", run_time=Decimal("2.12"), success=True,
        cmd="agg", service="web", env="prod", project="search_app", cluster="main",
        keywords=("검색어A", "검색어B"), company="acme", user="user01@example.com",
    )
    return QueryLogEntry(**{**base, **kw})


def _metric(**kw) -> NodeMetricEntry:
    base = dict(
        timestamp=TS, node_name="node-b02", node_ip="10.0.0.12",
        os_cpu_percent=1, os_mem_used_percent=99, process_cpu_percent=0,
        jvm_heap_used_percent=59, search_active=0, search_queue=0, search_rejected=0,
        write_active=0, write_queue=0, write_rejected=0,
    )
    return NodeMetricEntry(**{**base, **kw})


def test_all_three_are_log_entries():
    # fetch_logs가 셋을 한 리스트에 담아 돌려주고, split_by_minute이 그것을
    # timestamp로 묶는다. 공통 상위 타입이 그 계약이다.
    for e in (_slowlog(), _querylog(), _metric()):
        assert isinstance(e, LogEntry)
        assert e.timestamp == TS


def test_source_is_fixed_per_type_not_passed_in():
    # 예전에는 source를 생성자에 매번 넘겨야 했다 — 오타 한 번이면
    # build_minute_prompt의 소스별 묶기가 조용히 어긋난다.
    assert SlowlogEntry.source == "slowlog"
    assert QueryLogEntry.source == "es_query_log"
    assert NodeMetricEntry.source == "node_metric"
    assert _slowlog().source == "slowlog"


def test_values_stay_typed_instead_of_becoming_text():
    q = _querylog()
    assert q.run_time == Decimal("2.12")
    assert q.success is True
    assert q.keywords == ("검색어A", "검색어B")

    m = _metric(os_cpu_percent=94, search_queue=920)
    assert m.os_cpu_percent == 94
    assert m.search_queue == 920

    s = _slowlog()
    assert s.total_shards == 902


def test_failed_query_is_a_bool_not_a_letter():
    # ClickHouse는 'Y'/'N'을 준다. 도메인까지 그 표현을 끌고 오지 않는다.
    assert _querylog(success=False).success is False


def test_entries_are_immutable():
    with pytest.raises(FrozenInstanceError):
        _slowlog().took = "1s"


def test_keywords_are_hashable_so_entries_can_be_deduped():
    # list를 필드로 두면 frozen이어도 해시가 깨진다.
    assert hash(_querylog()) == hash(_querylog())
