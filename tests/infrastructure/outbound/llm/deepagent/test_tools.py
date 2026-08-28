"""analyze_logs가 agent에게 받은 ISO 시각을 어떻게 해석하는지 검증한다.

프롬프트는 KST를 지시하지만 모델은 지시를 어길 수 있다. 오프셋이 붙은
문자열이 왔을 때 ``replace(tzinfo=)``로 덮어쓰면 같은 벽시계가 다른 순간이
되어 정확히 9시간 어긋난 구간을 조회하게 된다 -- 조회는 성공하고 결과만
틀리므로 어디서도 드러나지 않는다.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from cluster_doctor.infrastructure.outbound.llm.deepagent.tools import _parse_kst

KST = timezone(timedelta(hours=9))


def test_naive_iso_is_read_as_kst_wall_clock():
    # 프롬프트가 지시하는 정상 경로. 오프셋이 없으면 KST 벽시계로 읽는다.
    assert _parse_kst("2026-08-27T18:33:00") == datetime(2026, 8, 27, 18, 33, tzinfo=KST)


def test_utc_designator_is_converted_not_overwritten():
    # "...Z"는 UTC 09:33 = KST 18:33. replace()로 덮어쓰면 KST 09:33이 되어
    # 9시간 어긋난다.
    assert _parse_kst("2026-08-27T09:33:00Z") == datetime(2026, 8, 27, 18, 33, tzinfo=KST)


def test_utc_offset_is_converted_not_overwritten():
    assert _parse_kst("2026-08-27T09:33:00+00:00") == datetime(2026, 8, 27, 18, 33, tzinfo=KST)


def test_kst_offset_passes_through_unchanged():
    assert _parse_kst("2026-08-27T18:33:00+09:00") == datetime(2026, 8, 27, 18, 33, tzinfo=KST)


def test_result_is_always_timezone_aware():
    # naive가 새어 나가면 ClickHouse 바인딩이 서버 tz 변환을 건너뛰고,
    # TimeRange가 aware/naive 혼재를 InvalidTimeRangeError로 거부한다.
    for iso in ("2026-08-27T18:33:00", "2026-08-27T09:33:00Z", "2026-08-27T18:33:00+09:00"):
        assert _parse_kst(iso).utcoffset() == timedelta(hours=9), iso


def _tools(fetch_logs=None, drain_pending=None, cluster=None):
    """이름 → tool 매핑. make_tools 호출마다 클로저 상태가 새로 만들어진다."""
    from cluster_doctor.infrastructure.outbound.llm.deepagent.tools import make_tools

    built = make_tools(
        cluster=cluster or MagicMock(),
        fetch_logs=fetch_logs or MagicMock(return_value=[]),
        drain_pending=drain_pending or (lambda: []),
        call_llm=MagicMock(return_value="report"),
        call_llm_minute=MagicMock(return_value='{"summary": "s", "evidence": []}'),
    )
    return {t.name: t for t in built}


def _analyze_logs_tool(fetch_logs):
    return _tools(fetch_logs=fetch_logs)["analyze_logs"]


def test_analyze_logs_passes_a_kst_window_to_the_repository():
    fetch_logs = MagicMock(return_value=[])
    tool = _analyze_logs_tool(fetch_logs)

    tool.invoke({"start_iso": "2026-08-27T09:30:00Z", "end_iso": "2026-08-27T09:35:00Z"})

    time_range = fetch_logs.call_args.args[0]
    assert time_range.start == datetime(2026, 8, 27, 18, 30, tzinfo=KST)
    assert time_range.end == datetime(2026, 8, 27, 18, 35, tzinfo=KST)


def test_analyze_logs_rejects_an_unparsable_time_without_raising():
    # tool에서 예외가 새면 agent 실행 전체가 중단된다.
    tool = _analyze_logs_tool(MagicMock(return_value=[]))
    result = tool.invoke({"start_iso": "not-a-time", "end_iso": "2026-08-27T18:35:00"})
    assert "파싱 오류" in result


# --------------------------------------------------------------------------
# sleep — 유입 대기 예산
#
# 새 워크플로우에서 sleep은 "slowlog 유입이 멎기를 기다리는" 루프의 일부다.
# 프롬프트가 상한을 지시하더라도 모델은 그것을 어길 수 있고, 그동안 분석은
# 시작조차 되지 않은 채 큐만 쌓인다. 그래서 예산을 tool이 강제한다.
# --------------------------------------------------------------------------

SLEEP_PATH = "cluster_doctor.infrastructure.outbound.llm.deepagent.tools.time.sleep"


def test_sleep_clamps_a_single_overlong_request():
    tool = _tools()["sleep"]
    with patch(SLEEP_PATH) as slept:
        result = tool.invoke({"seconds": 600})
    assert slept.call_args.args[0] == 60
    assert "60" in result


def test_sleep_reports_when_the_cumulative_cap_is_reached():
    tool = _tools()["sleep"]
    with patch(SLEEP_PATH):
        for _ in range(4):
            interim = tool.invoke({"seconds": 60})
            assert "상한" not in interim, interim
        final = tool.invoke({"seconds": 60})
    assert "상한" in final
    assert "analyze_logs" in final


def test_sleep_past_the_cap_does_not_actually_wait():
    # 상한에 닿은 뒤에도 모델이 sleep을 계속 부를 수 있다. 그때 실제로 자면
    # 상한이 의미가 없어진다.
    tool = _tools()["sleep"]
    with patch(SLEEP_PATH) as slept:
        for _ in range(5):
            tool.invoke({"seconds": 60})
        slept.reset_mock()
        result = tool.invoke({"seconds": 60})
    slept.assert_not_called()
    assert "상한" in result


def test_wait_budget_starts_fresh_for_each_agent_run():
    # make_tools는 analyze() 호출마다 새로 불린다. 이전 실행이 쓴 예산이
    # 남아 있으면 다음 사고에서 대기를 아예 못 한다.
    with patch(SLEEP_PATH):
        first = _tools()["sleep"]
        for _ in range(5):
            first.invoke({"seconds": 60})
        second = _tools()["sleep"]
        result = second.invoke({"seconds": 60})
    assert "상한" not in result, result


# --------------------------------------------------------------------------
# check_new_slowlogs — 유입 판정
# --------------------------------------------------------------------------

def _entries(*times):
    from cluster_doctor.domain.model.log_entry import SlowlogEntry

    return [SlowlogEntry(timestamp=t) for t in times]


def test_check_new_slowlogs_summarises_the_drained_batch():
    # 유입 판정에 필요한 것은 건수와 양 끝 시각뿐이다. timestamp를 전량
    # 돌려주면 유입이 몰릴 때 수백 건이 프롬프트에 실린다.
    drained = _entries(
        datetime(2026, 8, 27, 18, 33, 5, tzinfo=KST),
        datetime(2026, 8, 27, 18, 32, 50, tzinfo=KST),
        datetime(2026, 8, 27, 18, 33, 1, tzinfo=KST),
    )
    tool = _tools(drain_pending=lambda: drained)["check_new_slowlogs"]

    result = tool.invoke({})

    assert result["count"] == 3
    assert result["earliest"] == datetime(2026, 8, 27, 18, 32, 50, tzinfo=KST).isoformat()
    assert result["latest"] == datetime(2026, 8, 27, 18, 33, 5, tzinfo=KST).isoformat()


def test_check_new_slowlogs_reports_an_empty_queue_as_zero():
    # 2회 연속 이것이 나오면 유입이 멎은 것으로 본다.
    result = _tools()["check_new_slowlogs"].invoke({})
    assert result == {"count": 0, "earliest": None, "latest": None}


# --------------------------------------------------------------------------
# ES 조회는 ClusterRepository 포트를 거친다.
#
# 이전에는 raw Elasticsearch 클라이언트를 그대로 주입받아 tool 안에서
# es_client.cluster.health()를 직접 불렀다. 포트와 어댑터가 정의돼 있는데도
# 아무도 조립하지 않아, 어댑터의 health()와 tool 본문이 같은 코드로 중복돼
# 있었고 실행되는 쪽은 tool이었다.
# --------------------------------------------------------------------------

def test_cluster_health_tool_goes_through_the_port():
    cluster = MagicMock()
    cluster.health.return_value = {"status": "yellow"}

    result = _tools(cluster=cluster)["cluster_health"].invoke({})

    assert result == {"status": "yellow"}
    cluster.health.assert_called_once_with()


def test_explain_unassigned_shards_tool_goes_through_the_port():
    cluster = MagicMock()
    cluster.explain_allocation.return_value = {"can_allocate": "no"}

    result = _tools(cluster=cluster)["explain_unassigned_shards"].invoke({})

    assert "can_allocate" in result
    cluster.explain_allocation.assert_called_once_with()


def test_explain_unassigned_shards_reports_failure_as_an_observation():
    # 미할당 샤드가 없으면 ES가 예외를 던진다. tool에서 예외가 새면 agent
    # 실행 전체가 중단되므로 관찰 결과 문자열로 바꿔 돌려준다.
    cluster = MagicMock()
    cluster.explain_allocation.side_effect = RuntimeError("no unassigned shards")

    result = _tools(cluster=cluster)["explain_unassigned_shards"].invoke({})

    assert "미할당 샤드 없음" in result


def test_get_index_summary_tool_goes_through_the_port():
    cluster = MagicMock()
    cluster.index_summary.return_value = [{"index": "logs-1"}]

    result = _tools(cluster=cluster)["get_index_summary"].invoke({"index_pattern": "logs-*"})

    assert result == [{"index": "logs-1"}]
    cluster.index_summary.assert_called_once_with("logs-*")
