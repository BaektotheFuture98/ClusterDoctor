"""analyze_logs가 agent에게 받은 ISO 시각을 어떻게 해석하는지 검증한다.

프롬프트는 KST를 지시하지만 모델은 지시를 어길 수 있다. 오프셋이 붙은
문자열이 왔을 때 ``replace(tzinfo=)``로 덮어쓰면 같은 벽시계가 다른 순간이
되어 정확히 9시간 어긋난 구간을 조회하게 된다 -- 조회는 성공하고 결과만
틀리므로 어디서도 드러나지 않는다.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from cluster_doctor.infrastructure.outbound.llm.deepagent.diagnosis_state import (
    DiagnosisState,
)
from cluster_doctor.infrastructure.outbound.llm.deepagent.time_window import parse_kst

KST = timezone(timedelta(hours=9))


def test_naive_iso_is_read_as_kst_wall_clock():
    # 프롬프트가 지시하는 정상 경로. 오프셋이 없으면 KST 벽시계로 읽는다.
    assert parse_kst("2026-08-27T18:33:00") == datetime(2026, 8, 27, 18, 33, tzinfo=KST)


def test_utc_designator_is_converted_not_overwritten():
    # "...Z"는 UTC 09:33 = KST 18:33. replace()로 덮어쓰면 KST 09:33이 되어
    # 9시간 어긋난다.
    assert parse_kst("2026-08-27T09:33:00Z") == datetime(2026, 8, 27, 18, 33, tzinfo=KST)


def test_utc_offset_is_converted_not_overwritten():
    assert parse_kst("2026-08-27T09:33:00+00:00") == datetime(2026, 8, 27, 18, 33, tzinfo=KST)


def test_kst_offset_passes_through_unchanged():
    assert parse_kst("2026-08-27T18:33:00+09:00") == datetime(2026, 8, 27, 18, 33, tzinfo=KST)


def test_result_is_always_timezone_aware():
    # naive가 새어 나가면 ClickHouse 바인딩이 서버 tz 변환을 건너뛰고,
    # TimeRange가 aware/naive 혼재를 InvalidTimeRangeError로 거부한다.
    for iso in ("2026-08-27T18:33:00", "2026-08-27T09:33:00Z", "2026-08-27T18:33:00+09:00"):
        assert parse_kst(iso).utcoffset() == timedelta(hours=9), iso


_LOG_TIME = datetime(2026, 8, 27, 18, 30, tzinfo=timezone(timedelta(hours=9)))


def _tools(
    fetch_logs=None,
    drain_pending=None,
    cluster=None,
    state=None,
    node_log_fetcher=None,
    fetch_node_logs=None,
    call_llm=None,
    call_llm_minute=None,
    log_time=None,
    kafka_receive_time=None,
):
    """이름 → tool 매핑. make_tools 호출마다 클로저 상태가 새로 만들어진다."""
    from cluster_doctor.infrastructure.outbound.llm.deepagent.tools import make_tools

    if node_log_fetcher is None:
        # fetch가 빈 문자열을 돌려주게 못 박는다. MagicMock 기본 반환값을
        # 그대로 두면 master_logs에 MagicMock이 실려 종합 프롬프트에 그
        # repr이 들어간다 — SSH 수집을 건너뛴 경우와 같은 빈 문자열이 맞다.
        node_log_fetcher = MagicMock()
        node_log_fetcher.fetch.return_value = ""

    if fetch_node_logs is None:
        # 같은 이유로 빈 리스트를 못 박는다. MagicMock을 그대로 두면
        # format_log_line이 MagicMock에 걸려 TypeError를 낸다(등록되지 않은
        # 타입은 조용히 넘기지 않고 터뜨리도록 되어 있다).
        fetch_node_logs = MagicMock(return_value=[])

    if state is None:
        state = DiagnosisState(log_time or _LOG_TIME, kafka_receive_time or _LOG_TIME)

    built = make_tools(
        cluster=cluster or MagicMock(),
        fetch_logs=fetch_logs or MagicMock(return_value=[]),
        drain_pending=drain_pending or (lambda: []),
        call_llm=call_llm or MagicMock(return_value="report"),
        call_llm_minute=call_llm_minute
        or MagicMock(return_value='{"summary": "s", "evidence": []}'),
        node_log_fetcher=node_log_fetcher,
        fetch_node_logs=fetch_node_logs,
        state=state,
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
# analyze_logs — 실패 표식
#
# tool은 실패해도 예외를 올리지 않고 문자열을 돌려준다(예외는 agent 실행
# 전체를 죽인다). 그것만으로는 호출자가 분석 실패를 알 수 없어, agent가 쓴
# "분석하지 못했다" 리포트가 성공으로 취급된다. state에 남겨 알린다.
# --------------------------------------------------------------------------

def test_a_failed_analysis_that_is_never_retried_is_a_failed_diagnosis():
    state = DiagnosisState(_LOG_TIME, _LOG_TIME)
    fetch_logs = MagicMock(side_effect=RuntimeError("ClickHouse 접속 불가"))
    tool = _tools(fetch_logs=fetch_logs, state=state)["analyze_logs"]

    result = tool.invoke({"start_iso": "2026-08-27T18:30:00", "end_iso": "2026-08-27T18:35:00"})

    assert "오류" in result
    assert state.unresolved_failure() is not None


def test_a_failure_that_a_retry_resolved_is_not_a_failed_diagnosis():
    """일시 오류 뒤 같은 구간 재호출이 성공하면 진단은 성립한 것이다.

    첫 실패가 state.degraded를 박으면 되돌릴 방법이 없다. 실측
    (13:54 status=529 실패 → 14:00 같은 구간 재호출 → 14:04 성공)에서 리포트에
    붉은 "분석 실패" 배너가 붙고 재트리거까지 막혔다. 거짓 배너는 배너 전체의
    신뢰를 깎는다.
    """
    from cluster_doctor.domain.model.log_entry import SlowlogEntry

    state = DiagnosisState(_LOG_TIME, _LOG_TIME)
    logs = [SlowlogEntry(timestamp=datetime(2026, 8, 27, 18, 31, tzinfo=KST))]
    # 1회는 터지고 2회는 성공한다 — provider 과부하가 하는 그대로다.
    fetch_logs = MagicMock(side_effect=[RuntimeError("provider 과부하"), logs])
    tool = _tools(fetch_logs=fetch_logs, state=state)["analyze_logs"]

    window = {"start_iso": "2026-08-27T18:30:00", "end_iso": "2026-08-27T18:35:00"}
    assert "오류" in tool.invoke(window)
    tool.invoke(window)

    assert state.unresolved_failure() is None


def test_an_empty_window_is_not_a_degraded_run():
    # 로그가 없는 구간은 유효한 관찰 결과지 실패가 아니다. 이것을 실패로
    # 표시하면 한산한 시간대의 정상 진단이 통째로 실패로 승격된다.
    state = DiagnosisState(_LOG_TIME, _LOG_TIME)
    tool = _tools(fetch_logs=MagicMock(return_value=[]), state=state)["analyze_logs"]

    result = tool.invoke({"start_iso": "2026-08-27T18:30:00", "end_iso": "2026-08-27T18:35:00"})

    assert "로그 없음" in result
    assert state.degraded is False


# --------------------------------------------------------------------------
# analyze_logs — 호출 예산
#
# 호출 하나가 구간의 분 수만큼 LLM을 부르므로 가장 비싼 도구다. sleep과 같은
# 이유로 tool이 직접 막는다 — 프롬프트가 재시도를 제한해도 모델은 그것을
# 어길 수 있고, recursion_limit은 9,999라 프레임워크도 막아 주지 않는다.
# --------------------------------------------------------------------------

_WINDOW = {"start_iso": "2026-08-27T18:30:00", "end_iso": "2026-08-27T18:35:00"}


def test_analyze_logs_refuses_past_the_call_cap():
    from cluster_doctor.infrastructure.outbound.llm.deepagent.tools import (
        _MAX_ANALYZE_CALLS,
    )

    fetch_logs = MagicMock(return_value=[])
    tool = _analyze_logs_tool(fetch_logs)

    for _ in range(_MAX_ANALYZE_CALLS):
        interim = tool.invoke(_WINDOW)
        assert "상한" not in interim, interim

    fetch_logs.reset_mock()
    result = tool.invoke(_WINDOW)

    assert "상한" in result
    # 상한을 넘긴 호출은 조회조차 하지 않아야 한다. 조회 후에 막으면
    # 상한이 비용을 줄이지 못한다.
    fetch_logs.assert_not_called()


def test_analyze_call_budget_starts_fresh_for_each_agent_run():
    # make_tools는 analyze() 호출마다 새로 불린다. 이전 실행이 쓴 예산이
    # 남아 있으면 다음 사고에서 분석을 아예 못 한다.
    from cluster_doctor.infrastructure.outbound.llm.deepagent.tools import (
        _MAX_ANALYZE_CALLS,
    )

    first = _analyze_logs_tool(MagicMock(return_value=[]))
    for _ in range(_MAX_ANALYZE_CALLS + 1):
        first.invoke(_WINDOW)

    second = _analyze_logs_tool(MagicMock(return_value=[]))
    result = second.invoke(_WINDOW)

    assert "상한" not in result, result


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
    assert result["earliest"] == "2026-08-27T18:32:50"
    assert result["latest"] == "2026-08-27T18:33:05"
    # 누적값도 같은 호출에서 갱신된다. agent가 기억으로 들고 있지 않아야 한다.
    # first_seen은 트리거 시각(18:30)과 관측된 earliest 중 이른 쪽이다 —
    # 여기서는 트리거 시각이 더 이르므로 그쪽이 유입 시작이다.
    assert result["first_seen"] == "2026-08-27T18:30:00"
    assert result["last_seen"] == "2026-08-27T18:33:05"


def test_check_new_slowlogs_reports_an_empty_queue_as_zero():
    result = _tools()["check_new_slowlogs"].invoke({})

    assert result["count"] == 0
    assert result["earliest"] is None
    assert result["latest"] is None
    # 유입이 멎었다는 판정은 코드가 센다. 1회로는 멎은 것이 아니다 —
    # 커넥터 폴링 주기 때문에 유입 중에도 한 번은 0건이 나온다.
    assert result["zero_streak"] == 1
    assert result["inflow_settled"] is False
    # 제안 구간은 멎은 뒤에만 싣는다. 진행 중인 동안의 제안은 무의미하고
    # 토큰만 든다.
    assert "suggested_windows" not in result


# --------------------------------------------------------------------------
# ES 조회는 ClusterRepository 포트를 거친다.
#
# raw Elasticsearch 클라이언트를 그대로 주입받으면 tool이 es_client를 직접
# 부르게 된다. 포트와 어댑터가 정의돼 있어도 아무도 조립하지 않으면 어댑터의
# health()와 tool 본문이 같은 코드로 중복되고, 실행되는 쪽은 tool이다.
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


_NODE_LOG_WINDOW = {"start_iso": "2026-09-10T02:00:00", "end_iso": "2026-09-10T02:15:00"}


def _node_log_entry(**overrides):
    from cluster_doctor.domain.model.log_entry import NodeLogEntry

    fields = {
        "timestamp": datetime(2026, 9, 10, 2, 4, 33, tzinfo=timezone(timedelta(hours=9))),
        "node": "es-data-02",
        "node_role": "data",
        "level": "WARN ",
        "detected_level": "warn",
        "logger": "o.e.i.b.HierarchyCircuitBreakerService",
        "filename": "es-prod.log",
        "host": "es-node-02.internal",
        "line": "[gc][young] duration [1.2s]",
    }
    fields.update(overrides)
    return NodeLogEntry(**fields)


def test_search_node_logs_passes_every_condition_to_the_repository():
    # levels는 LLM이 다루기 쉬운 쉼표 문자열로 받아 튜플로 바꿔 넘긴다.
    fetch_node_logs = MagicMock(return_value=[])

    _tools(fetch_node_logs=fetch_node_logs)["search_node_logs"].invoke({
        "start_iso": "2026-09-10T02:00:00",
        "end_iso": "2026-09-10T02:15:00",
        "node": " es-data-02 ",
        "node_role": "data",
        "levels": "WARN,ERROR",
        "keyword": " heap ",
        "max_lines": 50,
    })

    args, kwargs = fetch_node_logs.call_args
    assert args[0].utcoffset() == timedelta(hours=9)
    assert args[1].utcoffset() == timedelta(hours=9)
    assert kwargs["node"] == "es-data-02"
    assert kwargs["node_role"] == "data"
    assert kwargs["levels"] == ("WARN", "ERROR")
    assert kwargs["keyword"] == "heap"
    assert kwargs["limit"] == 50


def test_search_node_logs_renders_entries_with_the_shared_formatter():
    fetch_node_logs = MagicMock(return_value=[_node_log_entry()])

    result = _tools(fetch_node_logs=fetch_node_logs)["search_node_logs"].invoke(_NODE_LOG_WINDOW)

    assert "es-data-02" in result
    assert "duration [1.2s]" in result
    assert "1줄" in result


def test_search_node_logs_reports_an_empty_result_as_an_observation():
    # 0건은 실패가 아니다. agent가 조건을 넓혀 다시 물어볼 수 있게 안내한다.
    result = _tools(fetch_node_logs=MagicMock(return_value=[]))["search_node_logs"].invoke(
        _NODE_LOG_WINDOW
    )

    assert "노드 로그 없음" in result
    assert "조건을 넓혀" in result


def test_search_node_logs_says_when_the_row_limit_truncated_the_result():
    fetch_node_logs = MagicMock(return_value=[_node_log_entry(), _node_log_entry()])
    payload = dict(_NODE_LOG_WINDOW, max_lines=2)

    result = _tools(fetch_node_logs=fetch_node_logs)["search_node_logs"].invoke(payload)

    assert "상한 2줄에 걸려" in result


def test_search_node_logs_failure_is_an_observation_not_a_degraded_run():
    # 보조 조사다. 실패해도 slowlog 분석 결과는 온전하므로 리포트를 버리지 않는다.
    state = DiagnosisState(_LOG_TIME, _LOG_TIME)
    fetch_node_logs = MagicMock(side_effect=RuntimeError("UNKNOWN_TABLE"))

    result = _tools(fetch_node_logs=fetch_node_logs, state=state)[
        "search_node_logs"
    ].invoke(_NODE_LOG_WINDOW)

    assert "노드 로그 조회 실패" in result
    assert state.degraded is False


def test_search_node_logs_rejects_an_unparsable_time_without_raising():
    result = _tools()["search_node_logs"].invoke(
        {"start_iso": "not-a-time", "end_iso": "2026-09-10T02:15:00"}
    )

    assert "시각 파싱 오류" in result


def test_analyze_logs_takes_master_logs_from_clickhouse_before_ssh():
    # 조회가 결과를 주면 SSH에 붙지 않는다. 접속 실패라는 실패 갈래를
    # 아예 만들지 않는 것이 이 전환의 목적이다.
    fetch_node_logs = MagicMock(return_value=[_node_log_entry(node_role="master")])
    node_log_fetcher = MagicMock()
    node_log_fetcher.fetch.return_value = ""

    _tools(
        fetch_logs=MagicMock(return_value=[_node_log_entry()]),
        fetch_node_logs=fetch_node_logs,
        node_log_fetcher=node_log_fetcher,
    )["analyze_logs"].invoke(_WINDOW)

    assert fetch_node_logs.call_args.kwargs["node_role"] == "master"
    node_log_fetcher.fetch.assert_not_called()


def test_analyze_logs_falls_back_to_ssh_when_the_table_is_not_ready_yet():
    fetch_node_logs = MagicMock(side_effect=RuntimeError("UNKNOWN_TABLE"))
    node_log_fetcher = MagicMock()
    node_log_fetcher.fetch.return_value = "[2026-09-10T02:04:33][WARN ] shard failed"

    _tools(
        fetch_logs=MagicMock(return_value=[_node_log_entry()]),
        fetch_node_logs=fetch_node_logs,
        node_log_fetcher=node_log_fetcher,
    )["analyze_logs"].invoke(_WINDOW)

    node_log_fetcher.fetch.assert_called_once()


def test_analyze_logs_proceeds_when_the_master_has_no_logs_at_all():
    # 마스터 로그는 보조 컨텍스트다. ClickHouse가 0건이고 SSH도 빈 문자열을
    # 돌려주는 경우(한산한 구간, 또는 그 구간에 WARN 이상이 없었던 경우)에도
    # slowlog 분석은 끝까지 진행되어야 한다.
    state = DiagnosisState(_LOG_TIME, _LOG_TIME)
    call_llm = MagicMock(return_value="===리포트===\n종합 결과")
    node_log_fetcher = MagicMock()
    node_log_fetcher.fetch.return_value = ""

    tools = _tools(
        fetch_logs=MagicMock(return_value=[_node_log_entry()]),
        fetch_node_logs=MagicMock(return_value=[]),
        node_log_fetcher=node_log_fetcher,
        state=state,
        call_llm=call_llm,
    )
    result = tools["analyze_logs"].invoke(_WINDOW)

    assert "종합 결과" in result
    assert state.degraded is False
    # 마스터 로그가 비면 종합 프롬프트에 해당 구획을 아예 넣지 않는다.
    # 빈 구획을 넣으면 모델이 "로그가 없었다"와 "수집하지 못했다"를 구별할 수 없다.
    synthesis_prompt = call_llm.call_args.args[0][0]["content"]
    assert "마스터 노드 로그" not in synthesis_prompt


def test_analyze_logs_proceeds_when_the_master_node_has_no_reachable_ip():
    # node_info가 빈 dict를 주거나 ip가 없으면(마스터 조회 실패, filter_path
    # 변경 등) SSH 폴백을 건너뛴다. 그때도 분석은 계속된다.
    state = DiagnosisState(_LOG_TIME, _LOG_TIME)
    cluster = MagicMock()
    cluster.node_info.return_value = {}
    node_log_fetcher = MagicMock()

    tools = _tools(
        fetch_logs=MagicMock(return_value=[_node_log_entry()]),
        fetch_node_logs=MagicMock(side_effect=RuntimeError("UNKNOWN_TABLE")),
        node_log_fetcher=node_log_fetcher,
        cluster=cluster,
        state=state,
    )
    result = tools["analyze_logs"].invoke(_WINDOW)

    node_log_fetcher.fetch.assert_not_called()
    assert "오류" not in result
    assert state.degraded is False


def test_analyze_logs_asks_for_cluster_event_loggers_not_just_warn_and_error():
    # 샤드 재배치·노드 이탈·allocation은 ES가 INFO로 남긴다. 레벨만 걸면
    # 이 수집의 목적인 이벤트가 통째로 빠진다.
    from cluster_doctor.infrastructure.outbound.llm.deepagent.tools import (
        _MASTER_EVENT_LOGGERS,
        _MASTER_LOG_MAX_LINES,
    )

    fetch_node_logs = MagicMock(return_value=[])

    _tools(
        fetch_logs=MagicMock(return_value=[_node_log_entry()]),
        fetch_node_logs=fetch_node_logs,
    )["analyze_logs"].invoke(_WINDOW)

    kwargs = fetch_node_logs.call_args.kwargs
    assert kwargs["node_role"] == "master"
    assert kwargs["levels"] == ("WARN", "ERROR")
    assert "o.e.c.r.a.AllocationService" in kwargs["loggers"]
    assert "o.e.c.s.MasterService" in kwargs["loggers"]
    assert kwargs["loggers"] == _MASTER_EVENT_LOGGERS
    # 로거를 좁혔으므로 상한도 함께 내렸다. 사고 중 비용 천장을 낮게 둔다.
    assert kwargs["limit"] == _MASTER_LOG_MAX_LINES == 80


# --------------------------------------------------------------------------
# search_node_logs — 클러스터 사건 로거 자동 포함
#
# 프롬프트 5c가 이 tool로 "shard 재배치·노드 이탈·리더 선출·allocation 실패"를
# 찾으라고 지시하는데 ES는 그것들을 INFO로 남긴다. 기본 levels="WARN,ERROR"만
# 걸면 0건이 돌아와 "특이사항 없음"으로 보고된다.
# --------------------------------------------------------------------------

def test_search_node_logs_adds_the_cluster_event_loggers_by_default():
    from cluster_doctor.infrastructure.outbound.llm.deepagent.tools import (
        _MASTER_EVENT_LOGGERS,
    )

    fetch_node_logs = MagicMock(return_value=[])

    _tools(fetch_node_logs=fetch_node_logs)["search_node_logs"].invoke(_NODE_LOG_WINDOW)

    kwargs = fetch_node_logs.call_args.kwargs
    assert kwargs["levels"] == ("WARN", "ERROR")
    assert kwargs["loggers"] == _MASTER_EVENT_LOGGERS


def test_emptying_levels_drops_the_logger_filter_too():
    # levels를 비우는 것은 "조건을 풀겠다"는 뜻이다. 로거를 남기면 비워도
    # 화이트리스트 밖은 못 보게 되어 프롬프트의 "조건을 하나씩 풀어 다시
    # 조회한다"가 성립하지 않는다.
    fetch_node_logs = MagicMock(return_value=[])
    payload = dict(_NODE_LOG_WINDOW, levels="")

    _tools(fetch_node_logs=fetch_node_logs)["search_node_logs"].invoke(payload)

    kwargs = fetch_node_logs.call_args.kwargs
    assert kwargs["levels"] == ()
    assert kwargs["loggers"] == ()


def test_search_node_logs_reports_truncation_against_the_clamped_limit():
    # 모델이 상한을 넘겨 부르면(자주 그런다) 요청값과 비교하는 순간 절단을
    # 놓친다. 어댑터가 2000으로 조이므로 2000건이면 잘린 것이다.
    from cluster_doctor.application.port.outbound.log_repository import (
        MAX_NODE_LOG_LIMIT,
    )

    entries = [_node_log_entry()] * MAX_NODE_LOG_LIMIT
    payload = dict(_NODE_LOG_WINDOW, max_lines=5000)

    result = _tools(fetch_node_logs=MagicMock(return_value=entries))[
        "search_node_logs"
    ].invoke(payload)

    assert f"상한 {MAX_NODE_LOG_LIMIT}줄에 걸려" in result
    assert "상한 5000줄" not in result


def test_zero_max_lines_does_not_claim_a_zero_line_cap():
    result = _tools(fetch_node_logs=MagicMock(return_value=[_node_log_entry()]))[
        "search_node_logs"
    ].invoke(dict(_NODE_LOG_WINDOW, max_lines=0))

    assert "상한 0줄" not in result


def test_search_node_logs_clamps_the_limit_it_asks_for():
    from cluster_doctor.application.port.outbound.log_repository import (
        MAX_NODE_LOG_LIMIT,
    )

    fetch_node_logs = MagicMock(return_value=[])

    _tools(fetch_node_logs=fetch_node_logs)["search_node_logs"].invoke(
        dict(_NODE_LOG_WINDOW, max_lines=99_999)
    )

    assert fetch_node_logs.call_args.kwargs["limit"] == MAX_NODE_LOG_LIMIT


# --------------------------------------------------------------------------
# analyze_logs — SSH 폴백은 실패에서만
#
# 로거를 좁히고 상한을 80으로 내렸으니 건강한 창에서 0건은 정상이다. 0건마다
# SSH로 내려가면 호출마다(최대 6회) ES 왕복 + 새 접속을 치르고, 이 전환으로
# 없앤 접속 실패 갈래를 정상 경로에 다시 들여놓는다.
# --------------------------------------------------------------------------

def test_zero_master_rows_does_not_trigger_the_ssh_fallback():
    node_log_fetcher = MagicMock()
    node_log_fetcher.fetch.return_value = ""
    cluster = MagicMock()

    _tools(
        fetch_logs=MagicMock(return_value=[_node_log_entry()]),
        fetch_node_logs=MagicMock(return_value=[]),
        node_log_fetcher=node_log_fetcher,
        cluster=cluster,
    )["analyze_logs"].invoke(_WINDOW)

    node_log_fetcher.fetch.assert_not_called()
    cluster.node_info.assert_not_called()


def test_a_failed_master_query_still_triggers_the_ssh_fallback():
    node_log_fetcher = MagicMock()
    node_log_fetcher.fetch.return_value = "[2026-09-10T02:04:33][WARN ] shard failed"

    _tools(
        fetch_logs=MagicMock(return_value=[_node_log_entry()]),
        fetch_node_logs=MagicMock(side_effect=RuntimeError("UNKNOWN_TABLE")),
        node_log_fetcher=node_log_fetcher,
    )["analyze_logs"].invoke(_WINDOW)

    node_log_fetcher.fetch.assert_called_once()


def test_get_node_info_is_gone():
    # get_node_logs가 내부에서 같은 조회를 하므로 agent가 부를 실익이 없었고,
    # 프롬프트에도 없었고, 호출 이력도 0회였다. 스키마 토큰만 먹고 있었다.
    assert "get_node_info" not in _tools()
    assert len(_tools()) == 7


# --------------------------------------------------------------------------
# 유입 구간의 관측과 커버리지
#
# first_seen·last_seen·zero_streak의 누적을 모델에게 맡기면 안 된다.
# check_new_slowlogs가 호출분만 돌려주고 누적을 하지 않으므로, 여러 호출에
# 걸친 값을 모델이 기억으로 들고 있게 된다. 틀려도 검증이 없고, 구간이
# 좁아지면 근거가 조용히 사라진다.
#
# 커버리지 판정에서 가장 조심할 것은 오경보다. 없는 문제를 보고하면 배너가
# 잡음이 되고, 잡음이 된 배너는 읽히지 않는다.
# --------------------------------------------------------------------------

def test_split_windows_that_together_cover_the_inflow_are_not_a_gap():
    """정당한 분할과 분 경계 반올림을 누락으로 보지 않는다.

    호출마다 따로 판정하면 분할이 전부 위반으로 잡힌다 — 첫 조각은
    ``end < last_seen``이고 둘째 조각은 ``start > first_seen``인 것이 당연하다.
    합집합으로만 봐야 한다.

    끝의 30초는 허용오차 안이다. agent가 구간을 분 경계로 반올림하므로 유입
    마지막 몇십 초가 밖으로 밀려나는 일이 정상적으로 생긴다.
    """
    trigger = datetime(2026, 8, 27, 18, 30, 10, tzinfo=KST)
    state = DiagnosisState(trigger, trigger)
    tools = _tools(
        drain_pending=lambda: _entries(
            trigger, datetime(2026, 8, 27, 18, 42, 30, tzinfo=KST)
        ),
        state=state,
    )
    tools["check_new_slowlogs"].invoke({})

    # 유입 18:30:10 ~ 18:42:30 을 두 조각으로 나눠 부른다.
    tools["analyze_logs"].invoke(
        {"start_iso": "2026-08-27T18:25:00", "end_iso": "2026-08-27T18:35:00"}
    )
    tools["analyze_logs"].invoke(
        {"start_iso": "2026-08-27T18:35:00", "end_iso": "2026-08-27T18:42:00"}
    )

    assert state.coverage_gaps() == []


def test_a_window_starting_after_the_inflow_began_is_reported_as_a_gap():
    # 구간을 좁게 잡는 것은 조용히 근거를 잃는 경로다. 지금까지는 흔적이
    # 남지 않았다 — 리포트에 분석 구간이 적히지만 그것이 유입을 감쌌는지는
    # 아무도 보지 않았다.
    trigger = datetime(2026, 8, 27, 18, 30, 0, tzinfo=KST)
    state = DiagnosisState(trigger, trigger)
    tools = _tools(
        drain_pending=lambda: _entries(
            trigger, datetime(2026, 8, 27, 18, 41, 50, tzinfo=KST)
        ),
        state=state,
    )
    tools["check_new_slowlogs"].invoke({})

    tools["analyze_logs"].invoke(
        {"start_iso": "2026-08-27T18:36:00", "end_iso": "2026-08-27T18:44:00"}
    )

    gaps = state.coverage_gaps()
    assert len(gaps) == 1
    assert "360초" in gaps[0], gaps[0]


def test_nothing_is_claimed_when_the_inflow_was_never_observed():
    # check_new_slowlogs를 한 번도 부르지 않으면 유입을 모른다. 근거 없이
    # 누락을 보고하지 않는다.
    state = DiagnosisState(_LOG_TIME, _LOG_TIME)
    assert state.coverage_gaps() == []  # last_seen이 기본값(None) 그대로다.

    state.first_seen = None
    assert state.coverage_gaps() == []  # first_seen까지 없어도 여전히 주장하지 않는다.


def test_two_empty_checks_settle_the_inflow_and_one_does_not():
    """유입이 멎었다는 판정을 코드가 센다.

    0건 한 번으로는 부족하다 — 커넥터 폴링 주기 때문에 유입이 계속되는 중에도
    한 번은 0건이 나온다. 중간에 유입이 있으면 다시 0에서 센다.
    """
    batches = [
        _entries(datetime(2026, 8, 27, 18, 30, 5, tzinfo=KST)),
        [],
        _entries(datetime(2026, 8, 27, 18, 31, 40, tzinfo=KST)),
        [],
        [],
    ]
    tool = _tools(drain_pending=lambda: batches.pop(0))["check_new_slowlogs"]

    assert tool.invoke({})["zero_streak"] == 0          # 유입 있음
    assert tool.invoke({})["inflow_settled"] is False   # 0건 1회
    assert tool.invoke({})["zero_streak"] == 0          # 다시 유입 → 리셋
    assert tool.invoke({})["inflow_settled"] is False   # 0건 1회
    settled = tool.invoke({})                            # 0건 2회
    assert settled["inflow_settled"] is True

    # 멎은 뒤에만 제안 구간이 실린다. 유입 시작 5분 전부터다.
    windows = settled["suggested_windows"]
    assert windows[0]["start_iso"] == "2026-08-27T18:25:00"
    assert all(w["start_iso"] < w["end_iso"] for w in windows)


# --------------------------------------------------------------------------
# 분석이 실패해도 코드가 아는 관측값은 살아남는다
#
# timeline_row·node_metric_summary·slow_candidates는 LLM을 전혀 타지 않는
# 순수 함수다. 이 수집이 _graph.invoke **뒤에** 있으면, 429로 분 단위 호출이
# 전부 실패할 때(synthesize가 LlmApiError를 올린다) 조회에 성공한 구간이
# 리포트에서 통째로 빈칸이 된다 — 운영자에게는 "아무 일도 없던 시간"으로
# 보인다. 429가 주 실패 모드라는 전제대로라면 한 구간의 모든
# 분이 함께 실패하는 것이 가장 흔한 실패 형태다.
# --------------------------------------------------------------------------

def _busy_logs(minute: datetime):
    """한 분에 세 소스가 다 들어 있는 버킷."""
    from decimal import Decimal

    from cluster_doctor.domain.model.log_entry import QueryLogEntry, SlowlogEntry
    from cluster_doctor.domain.model.node_metric import NodeMetricEntry

    return [
        SlowlogEntry(
            timestamp=minute,
            index_name="news-2026",
            node="es-data-01",
            took="12.5s",
            total_hits="9000",
            total_shards=30,
            query='{"query":{"match_all":{}}}',
        ),
        QueryLogEntry(
            timestamp=minute,
            host="10.0.0.9",
            run_time=Decimal("8.4"),
            success=True,
            cmd="agg",
            service="search",
            env="prod",
            project="p",
            cluster="c",
            keywords=("짐빔",),
            company="뉴엔AI",
            user="dhlee@newen.ai",
        ),
        NodeMetricEntry(
            timestamp=minute,
            node_name="es-data-01",
            node_ip="10.0.0.9",
            os_cpu_percent=71,
            os_mem_used_percent=88,
            process_cpu_percent=64,
            jvm_heap_used_percent=93,
            search_active=4,
            search_queue=120,
            search_rejected=17,
            write_active=1,
            write_queue=0,
            write_rejected=0,
        ),
    ]


def _analyze_with_every_minute_failing(state):
    """분 단위 LLM이 전부 실패하는 구간을 한 번 분석한다."""
    from cluster_doctor.application.port.outbound.llm_analyzer import LlmApiError

    minute = datetime(2026, 8, 27, 18, 31, tzinfo=KST)
    tool = _tools(
        fetch_logs=MagicMock(return_value=_busy_logs(minute)),
        call_llm_minute=MagicMock(side_effect=LlmApiError("429 rate limit")),
        state=state,
    )["analyze_logs"]
    result = tool.invoke(
        {"start_iso": "2026-08-27T18:30:00", "end_iso": "2026-08-27T18:35:00"}
    )
    return result, minute


def test_전구간_분석_실패에도_노드_메트릭과_후보는_남는다():
    state = DiagnosisState(_LOG_TIME, _LOG_TIME)
    result, _ = _analyze_with_every_minute_failing(state)

    assert "분석 실패" in result
    assert state.nodes, "조회에 성공한 노드 메트릭이 사라졌다"
    assert state.candidates, "느린 요청 후보가 사라졌다"


def test_분석에_실패한_분도_타임라인에_행을_남긴다():
    # 행 자체가 없으면 그 분이 타임라인에서 사라지고 "실패해서 못 봤다"가
    # "아무 일도 없었다"로 읽힌다. row.failed 배너도 행이 없으면 뜨지 않는다.
    state = DiagnosisState(_LOG_TIME, _LOG_TIME)
    _, minute = _analyze_with_every_minute_failing(state)

    assert minute in state.timeline, "실패한 분이 타임라인에서 통째로 빠졌다"
    row = state.timeline[minute]
    assert row.failed is True
    # 건수는 logs만으로 계산되므로 LLM이 실패해도 정확하다.
    assert row.counts["slowlog"] == 1
    assert row.counts["es_query_log"] == 1


def test_조회_자체가_실패하면_없는_로그로_타임라인을_지어내지_않는다():
    state = DiagnosisState(_LOG_TIME, _LOG_TIME)
    tool = _tools(
        fetch_logs=MagicMock(side_effect=RuntimeError("ClickHouse 접속 불가")),
        state=state,
    )["analyze_logs"]

    tool.invoke({"start_iso": "2026-08-27T18:30:00", "end_iso": "2026-08-27T18:35:00"})

    assert not state.timeline


# --------------------------------------------------------------------------
# 후보 목록은 모델에게 실제로 도달해야 한다
#
# 프롬프트는 "코드가 [C1] [C2] … 후보로 제시하므로 너는 id와 고른 이유만
# 쓴다"고 말한다. 그 목록을 보내는 경로가 없으면 모델의 candidate_id는
# 지어낸 값이 되고, 리포트의 조인이 100% 실패해 "선정 이유"가 한 번도
# 렌더되지 않는다 — 스키마 한 벌이 통째로 도달 불가능한 코드가 된다.
# --------------------------------------------------------------------------

def test_analyze_logs가_후보_id를_반환값에_실어_보낸다():
    minute = datetime(2026, 8, 27, 18, 31, tzinfo=KST)
    tool = _tools(
        fetch_logs=MagicMock(return_value=_busy_logs(minute)),
    )["analyze_logs"]

    result = tool.invoke(
        {"start_iso": "2026-08-27T18:30:00", "end_iso": "2026-08-27T18:35:00"}
    )

    assert "[C1]" in result
    # 수치도 함께 가야 모델이 "took을 직접 적지 마라"를 지킬 수 있다.
    assert "took=12.5s" in result


def test_후보_id는_리포트에_실리는_것과_같은_것이다():
    # 모델이 보는 id와 운영자가 보는 id가 갈리면 조인이 깨진다.
    state = DiagnosisState(_LOG_TIME, _LOG_TIME)
    minute = datetime(2026, 8, 27, 18, 31, tzinfo=KST)
    tool = _tools(
        fetch_logs=MagicMock(return_value=_busy_logs(minute)),
        state=state,
    )["analyze_logs"]

    result = tool.invoke(
        {"start_iso": "2026-08-27T18:30:00", "end_iso": "2026-08-27T18:35:00"}
    )

    for candidate in state.candidates.values():
        assert f"[{candidate.candidate_id}]" in result


# --------------------------------------------------------------------------
# 같은 구간을 두 번 부르지 않는다
# --------------------------------------------------------------------------

def test_이미_분석한_구간을_다시_요청하면_호출_예산을_쓰지_않는다():
    # 실측으로 16:09~16:17이 연달아 두 번 요청됐다. 반환값이 같으므로 새로
    # 얻는 것은 없고, 조회와 분 단위 LLM 호출 비용만 그대로 다시 든다.
    state = DiagnosisState(_LOG_TIME, _LOG_TIME)
    fetch_logs = MagicMock(return_value=_busy_logs(datetime(2026, 8, 27, 18, 31, tzinfo=KST)))
    tool = _tools(fetch_logs=fetch_logs, state=state)["analyze_logs"]
    window = {"start_iso": "2026-08-27T18:30:00", "end_iso": "2026-08-27T18:35:00"}

    tool.invoke(window)
    second = tool.invoke(window)

    assert "이미 분석했다" in second
    assert fetch_logs.call_count == 1
    # 거절된 요청은 분석이 아니므로 커버리지 입력에도 두 번 들어가지 않는다.
    assert state.requested.count(
        (
            datetime(2026, 8, 27, 18, 30, tzinfo=KST),
            datetime(2026, 8, 27, 18, 35, tzinfo=KST),
        )
    ) == 1


# --------------------------------------------------------------------------
# 클러스터 상태 파싱은 tool 밖으로 새면 안 된다
#
# 변경 전에는 cluster.health()만 예외원이었고 반환값을 파싱하지 않았다.
# 이제 tool 안에서 int()를 부르므로, ES가 오류 응답 본문이나 문자열을 주면
# 예외가 agent 실행 전체를 죽여 리포트까지 사라진다.
# --------------------------------------------------------------------------

def test_클러스터_상태_payload가_이상해도_tool은_예외를_내지_않는다():
    cluster = MagicMock()
    cluster.health.return_value = {
        "status": "red",
        "unassigned_shards": "많음",
        "active_shards": None,
        "number_of_nodes": {"뜻밖의": "dict"},
    }
    tool = _tools(cluster=cluster)["cluster_health"]

    result = tool.invoke({})

    assert result["status"] == "red"


def test_클러스터_상태_payload가_dict가_아니어도_tool은_예외를_내지_않는다():
    cluster = MagicMock()
    cluster.health.return_value = "서비스 사용 불가"
    tool = _tools(cluster=cluster)["cluster_health"]

    tool.invoke({})


# --------------------------------------------------------------------------
# clock skew — 발생 시각이 미래인 slowlog
#
# 노드 시계가 앞서 있으면 timestamp가 지금보다 뒤인 값으로 들어온다. 그대로
# 쓰면 last_seen이 미래가 되는데 first_seen은 _base_time이 clock skew를 잡아
# kafka_receive_time으로 눌러 둔 값이라 기준이 서로 달라진다. 실측에서 유입
# 구간이 13:14~16:16(3시간)으로 벌어져 커버리지 판정이 "10,200초를 못 봤다"고
# 말했다.
# --------------------------------------------------------------------------

def test_미래_시각의_slowlog는_유입_구간을_미래로_벌리지_않는다():
    from cluster_doctor.domain.model.log_entry import SlowlogEntry

    now = datetime.now(KST)
    future = now + timedelta(hours=3)
    tool = _tools(
        drain_pending=lambda: [SlowlogEntry(timestamp=future)],
        log_time=future,
        kafka_receive_time=now,
    )["check_new_slowlogs"]

    result = tool.invoke({})

    last_seen = datetime.strptime(result["last_seen"], "%Y-%m-%dT%H:%M:%S")
    assert last_seen <= now.replace(tzinfo=None) + timedelta(seconds=1)


# --------------------------------------------------------------------------
# SSH 폴백으로 온 마스터 로그도 구조를 갖는다
#
# 이쪽은 NodeLogEntry가 아니라 파일 원문 덩어리다. 레벨과 로거를 뽑지 않으면
# 리포트가 사건별로 묶을 때 쓰는 키가 모든 줄에 대해 같아져, 실측 312줄이
# 헤더 한 줄 + 본문 한 줄로 붕괴한다. 적재가 채워지는 중이라 이 폴백을 남겨
# 둔 것인데 정작 그 상황에서 리포트가 가장 빈약해지는 셈이었다.
# --------------------------------------------------------------------------

_SSH_LOG = (
    "[2026-08-27T18:31:02,415][WARN ][o.e.c.c.LagDetector      ] "
    "[es-master-01] node [{RC17-08}{abc}] is lagging\n"
    "[2026-08-27T18:31:03,001][ERROR][o.e.a.s.TransportSearchAction] "
    "[es-master-01] all shards failed\n"
    "        at org.elasticsearch.Foo.bar(Foo.java:42)\n"
)


def _analyze_with_ssh_master_logs(state):
    """ClickHouse 조회가 실패해 SSH로 내려가는 경로."""
    cluster = MagicMock()
    cluster.node_info.return_value = {
        "ip": "10.0.0.1", "log_path": "/var/log/es", "cluster_name": "prod"
    }
    node_log_fetcher = MagicMock()
    node_log_fetcher.fetch.return_value = _SSH_LOG

    tool = _tools(
        fetch_logs=MagicMock(return_value=_busy_logs(datetime(2026, 8, 27, 18, 31, tzinfo=KST))),
        fetch_node_logs=MagicMock(side_effect=RuntimeError("테이블 준비 안 됨")),
        cluster=cluster,
        node_log_fetcher=node_log_fetcher,
        state=state,
    )["analyze_logs"]
    tool.invoke({"start_iso": "2026-08-27T18:30:00", "end_iso": "2026-08-27T18:35:00"})
    return list(state.master_logs.values())


def test_ssh_폴백_로그에서_레벨과_로거를_뽑는다():
    events = _analyze_with_ssh_master_logs(DiagnosisState(_LOG_TIME, _LOG_TIME))

    by_logger = {e.logger for e in events}
    assert "o.e.c.c.LagDetector" in by_logger
    assert "o.e.a.s.TransportSearchAction" in by_logger
    assert {e.level for e in events} >= {"WARN", "ERROR"}


def test_ssh_폴백_로그의_시각도_뽑는다():
    events = _analyze_with_ssh_master_logs(DiagnosisState(_LOG_TIME, _LOG_TIME))

    stamped = [e for e in events if e.timestamp is not None]
    assert len(stamped) == 2
    assert stamped[0].timestamp == datetime(2026, 8, 27, 18, 31, 2, tzinfo=KST)


def test_머리를_뽑지_못한_줄도_버리지_않는다():
    # 스택 트레이스 연속 행. 값이 없다는 것과 줄이 없다는 것은 다르다.
    events = _analyze_with_ssh_master_logs(DiagnosisState(_LOG_TIME, _LOG_TIME))

    assert any("org.elasticsearch.Foo.bar" in e.line for e in events)
    assert len(events) == 3
