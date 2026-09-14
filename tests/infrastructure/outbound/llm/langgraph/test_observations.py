"""관측값을 계산하는 순수 함수들.

이 모듈에는 테스트가 없었고, 그 사이에 두 가지가 조용히 틀려 있었다 —
``parse_duration_ms``가 0ms를 파싱 실패와 같이 취급했고, 단위 표의 micros/nanos
항목이 잘라내기 때문에 늘 0을 돌려줬다. 순수 함수라 외부 의존 없이 도는데도
검증이 없었던 자리다.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from cluster_doctor.domain.model.log_entry import QueryLogEntry, SlowlogEntry
from cluster_doctor.domain.model.node_metric import NodeMetricEntry
from cluster_doctor.infrastructure.outbound.llm.langgraph.observations import (
    candidate_key,
    count_by_source,
    merge_node_rows,
    node_metric_summary,
    parse_duration_ms,
    slow_candidates,
    timeline_row,
)

KST = timezone(timedelta(hours=9))
_MINUTE = datetime(2026, 9, 10, 15, 27, tzinfo=KST)


def _slowlog(took: str, **kwargs) -> SlowlogEntry:
    return SlowlogEntry(timestamp=_MINUTE, took=took, **kwargs)


def _query(run_time: str, **kwargs) -> QueryLogEntry:
    return QueryLogEntry(
        timestamp=_MINUTE,
        host="10.0.0.1",
        run_time=Decimal(run_time),
        success=True,
        cmd="agg",
        service="search",
        env="prod",
        project="p",
        cluster="c",
        keywords=(),
        company=None,
        user=None,
        **kwargs,
    )


def _metric(node: str, **kwargs) -> NodeMetricEntry:
    defaults = dict(
        os_cpu_percent=0,
        os_mem_used_percent=0,
        process_cpu_percent=0,
        jvm_heap_used_percent=0,
        search_active=0,
        search_queue=0,
        search_rejected=0,
        write_active=0,
        write_queue=0,
        write_rejected=0,
    )
    defaults.update(kwargs)
    return NodeMetricEntry(
        timestamp=_MINUTE, node_name=node, node_ip="10.0.0.1", **defaults
    )


# ──────────────────────────── parse_duration_ms ────────────────────────────


def test_긴_접미사가_짧은_것보다_먼저_읽힌다():
    # "ms"를 "m"으로 읽으면 500밀리초가 500분이 되어 최댓값 비교가 뒤집힌다.
    assert parse_duration_ms("500ms") == 500
    assert parse_duration_ms("500m") == 30_000_000


def test_소수점과_공백이_섞여도_읽는다():
    assert parse_duration_ms("37.1s") == 37_100
    assert parse_duration_ms("  1.5 s  ") == 1_500


def test_읽을_수_없으면_None이지_0이_아니다():
    # 0과 None이 같아지면 "0ms였다"와 "읽지 못했다"가 구별되지 않는다.
    assert parse_duration_ms("zzz") is None
    assert parse_duration_ms("") is None
    assert parse_duration_ms("1.5 seconds") is None


def test_0에_가까운_값도_파싱_성공으로_돌려준다():
    assert parse_duration_ms("0.4ms") == 0
    assert parse_duration_ms("0s") == 0


def test_1ms_미만은_잘라내지_않고_반올림한다():
    # 잘라내면 0.9ms가 0이 되고 0.4ms와 순서가 뒤집힌다.
    assert parse_duration_ms("0.9ms") == 1
    assert parse_duration_ms("0.4ms") == 0


# ──────────────────────────── slow_candidates ────────────────────────────


def test_파싱된_0ms는_파싱_실패보다_앞에_온다():
    # ``or -1``이었을 때 0ms가 -1로 접혀 파싱 실패와 같은 자리로 밀렸다.
    logs = [
        _slowlog("읽을 수 없음", index_name="unparsable"),
        _slowlog("0.4ms", index_name="zero"),
    ]

    picked = slow_candidates(logs, limit=2)

    assert [c.index_name for c in picked] == ["zero", "unparsable"]


def test_두_소스를_각각_고른다():
    # 한쪽으로 합쳐 정렬하면 slowlog가 0건인 구간에서 쿼리 후보마저 밀려난다.
    logs = [_slowlog("10s", index_name="a"), _query("99.9")]

    picked = slow_candidates(logs, limit=1)

    assert {c.source for c in picked} == {"slowlog", "es_query_log"}


def test_slowlog가_없어도_쿼리_후보는_나온다():
    picked = slow_candidates([_query("8.0")], limit=3)

    assert len(picked) == 1
    assert picked[0].run_time == Decimal("8.0")


def test_후보_id는_비운_채_돌려준다():
    # id를 붙이는 것은 호출부의 일이다. 여러 analyze_logs 호출에 걸쳐 번호가
    # 이어져야 하는데 그 상태는 run_state가 갖고 있다.
    assert all(c.candidate_id == "" for c in slow_candidates([_slowlog("1s")]))


def test_후보_동일성은_내용으로_판정한다():
    # 겹친 구간을 다시 조회해 같은 요청이 또 나와도 새 번호를 주면 안 된다.
    one = slow_candidates([_slowlog("1s", node="es-01")])[0]
    two = slow_candidates([_slowlog("1s", node="es-01")])[0]

    assert candidate_key(one) == candidate_key(two)


# ──────────────────────────── timeline_row ────────────────────────────


def test_소스별_건수가_갈려_있다():
    # 한 칸으로 뭉쳤을 때 es_query_log 264건이 slowlog 건수로 실렸다(실측).
    row = timeline_row(_MINUTE, [_slowlog("1s"), _query("2"), _query("3")])

    assert row.counts["slowlog"] == 1
    assert row.counts["es_query_log"] == 2


def test_빈_버킷도_행을_만든다():
    row = timeline_row(_MINUTE, [], failed=True)

    assert row.minute == _MINUTE
    assert row.failed is True
    assert row.counts == {}


def test_파싱되지_않는_took도_원문을_남긴다():
    # 숫자로 비교할 수 없다는 것이 값이 없다는 뜻은 아니다.
    row = timeline_row(_MINUTE, [_slowlog("알 수 없음")])

    assert row.took_max == "알 수 없음"
    assert row.took_max_ms is None


# ──────────────────────────── 노드 메트릭 ────────────────────────────


def test_노드별로_각_지표의_최대값을_잡는다():
    logs = [
        _metric("es-01", jvm_heap_used_percent=40, search_rejected=0),
        _metric("es-01", jvm_heap_used_percent=91, search_rejected=7),
    ]

    rows = node_metric_summary(logs)

    assert rows["es-01"].jvm_heap_max == 91
    assert rows["es-01"].search_rejected_max == 7
    assert rows["es-01"].samples == 2


def test_구간을_나눠_불러도_최대값의_최대값이_남는다():
    # analyze_logs가 여러 번 불리고 구간이 겹칠 수 있다.
    acc = node_metric_summary([_metric("es-01", os_cpu_percent=30)])
    merge_node_rows(acc, node_metric_summary([_metric("es-01", os_cpu_percent=88)]))
    merge_node_rows(acc, node_metric_summary([_metric("es-02", os_cpu_percent=5)]))

    assert acc["es-01"].cpu_max == 88
    assert acc["es-01"].samples == 2
    assert set(acc) == {"es-01", "es-02"}


def test_빈_병합은_기존_값을_지우지_않는다():
    acc = node_metric_summary([_metric("es-01", os_cpu_percent=30)])
    merge_node_rows(acc, {})

    assert acc["es-01"].cpu_max == 30


# ──────────────────────────── count_by_source ────────────────────────────


def test_소스가_없으면_빈_dict다():
    assert count_by_source([]) == {}
