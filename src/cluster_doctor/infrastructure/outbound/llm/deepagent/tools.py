"""DeepAgent tool 정의.

``make_tools(cluster, fetch_logs, drain_pending, call_llm, call_llm_minute, *, state=...)``
팩토리로 의존성을 클로저에 포획한다. ES 조회는 ClusterRepository 포트를 거친다 —
tool은 elasticsearch 클라이언트를 모른다. ``state``는 호출자가 소유하는
``DiagnosisState``로, tool이 실패를 문자열로 삼킬 때 그 사실을 호출자에게
남기는 통로다.

- analyze_logs(start_iso, end_iso): agent가 ISO 시각으로 구간 지정 → ClickHouse 조회 → LangGraph 분석 (구간 최대 10분, 진단당 최대 6회)
- check_new_slowlogs(): agent 실행 중 큐에 새로 쌓인 slowlog 확인
- search_node_logs(...): 마스터 노드 로그를 ClickHouse에서 검색
- get_node_logs(node_id, ...): 데이터 노드 로그를 SSH로 수집 (내부에서 GET /_nodes/<id>)
- cluster_health / explain_unassigned_shards: ES 직접 호출
- sleep: 대기

노드 로그의 출처가 둘로 갈리는 것은 적재 범위에서 온 것이다. ClickHouse에는
마스터 노드 로그만 들어오므로 데이터 노드는 SSH로 간다.
"""
from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import datetime, timedelta

from langchain_core.tools import tool

_logger = logging.getLogger(__name__)

from cluster_doctor.application.port.outbound.cluster_repository import ClusterRepository
from cluster_doctor.application.port.outbound.log_repository import (
    DEFAULT_NODE_LOG_LIMIT,
    clamp_node_log_limit,
)
from cluster_doctor.domain.model.log_entry import LogEntry, NodeLogEntry
from cluster_doctor.domain.model.time_range import (
    MAX_TIME_RANGE_DURATION,
    InvalidTimeRangeError,
    TimeRange,
)
from cluster_doctor.infrastructure.outbound.llm.langgraph.prompts import format_log_line
from cluster_doctor.application.port.outbound.llm_analyzer import (
    LlmApiError,
    LlmResponseError,
)
from cluster_doctor.infrastructure.outbound.llm.langgraph.graph import build_graph
from cluster_doctor.infrastructure.outbound.llm.langgraph.nodes import LlmCaller
from cluster_doctor.application.port.outbound.node_log_fetcher import (
    DEFAULT_HOST_LOG_LINES,
    NodeLogFetcher,
)
from cluster_doctor.infrastructure.outbound.llm.deepagent.diagnosis_state import (
    DiagnosisState,
)
from cluster_doctor.infrastructure.outbound.llm.deepagent.time_window import (
    KST as _KST,
    fmt as _fmt,
    parse_window as _parse_window,
)


def _render_entries(entries: list[NodeLogEntry]) -> str:
    """노드 로그 항목들을 프롬프트에 실을 여러 줄로 그린다.

    ``line``은 자르지 않는다. 스택 트레이스든 GC 통계든 진단에 쓰이는 것은
    원문 그대로이고, 잘라내면 근거로 인용할 수 없다. 양은 줄 수(``limit``)로
    묶는다.
    """
    return "\n".join(format_log_line(entry) for entry in entries)


# 분석 창 상한. 도메인의 TimeRange가 같은 제약을 강제하므로 값을 두 번 쓰지
# 않는다 — 두 곳에 박아 두면 한쪽만 고쳤을 때 tool은 통과시키고 TimeRange가
# 거부하며 서로 다른 오류 메시지를 낸다.
#
# 그래도 tool 쪽 검사를 남기는 이유는 반환 형태가 다르기 때문이다. tool은
# 모델이 읽고 스스로 고칠 수 있는 안내 문장을 돌려주고, 도메인은 어떤
# 호출자에게든 예외를 던진다.
_MAX_WINDOW_MINUTES = int(MAX_TIME_RANGE_DURATION.total_seconds() // 60)

# 유입 대기 예산. agent는 slowlog 유입이 멎을 때까지 sleep으로 기다리는데,
# 그동안 분석은 시작조차 되지 않고 큐만 쌓인다. 프롬프트가 상한을 지시해도
# 모델은 그것을 어길 수 있으므로 tool이 강제한다.
#
# 1회 상한을 60초로 둔 이유: 대기를 여러 번으로 쪼개야 매 사이클마다
# check_new_slowlogs로 유입 여부를 다시 볼 수 있다. 한 번에 5분을 자면
# 그 사이 유입이 멎어도 알아채지 못한다.
_MAX_SLEEP_SECONDS = 60
_MAX_WAIT_SECONDS = 300

# 한 번의 진단에서 analyze_logs를 부를 수 있는 횟수. 호출 하나가 구간의
# 분 수만큼 LLM을 부르므로(5분 창 실측 513,122 토큰) 가장 비싼 도구다.
# sleep과 같은 이유로 tool이 직접 막는다 — 프롬프트가 재시도를 한 번으로
# 제한해도 모델은 그것을 어길 수 있고, recursion_limit은 9,999라
# 프레임워크도 막아 주지 않는다. 10분 창을 10분 이하로 쪼개 부르는 경우와
# 허용된 재시도 1회를 합쳐도 6회면 넉넉하다.
_MAX_ANALYZE_CALLS = 6

# analyze_logs가 자동으로 붙이는 마스터 로그의 범위. node_role 값이 다르면
# (예: "master-eligible") 여기를 고쳐야 한다 — 틀리면 조회가 0건이 되고,
# 그때는 SSH 폴백이 대신 받는다. 실측 테이블에서는 "master"다.
_MASTER_ROLE = "master"

# 레벨만으로는 안 된다. 이 수집이 존재하는 목적은 클러스터 차원 사건(샤드
# 재배치, 노드 이탈, 리더 선출, allocation 실패)을 보는 것인데 ES는 그것들을
# INFO로 남긴다. 그렇다고 INFO를 열 수도 없다 — 실측(packetbeat.loki_logs)에서
# INFO 10건 중 진단에 필요한 것은 AllocationService 1건이었고 나머지 9건은
# ML 유지보수·만료 데이터 삭제·매핑 변경 잡음이었다. 사고 중에는 샤드별
# INFO가 폭증해 그 비율이 더 나빠지고, 300줄 상한을 잡음으로 채워 정작 원인
# 이벤트를 밀어낸다.
#
# 그래서 레벨 대신 로거로 고른다. 클러스터 사건을 내는 로거는 소수이고 사건당
# 요약 한 줄을 내므로, 커버리지는 올라가고 볼륨은 오히려 내려간다.
# 어댑터에서 levels와 OR로 묶이므로 WARN/ERROR는 로거와 무관하게 들어온다.
_MASTER_LOG_LEVELS = ("WARN", "ERROR")
_MASTER_EVENT_LOGGERS = (
    "o.e.c.s.MasterService",                   # 클러스터 상태 변경, node-left/join
    "o.e.c.c.Coordinator",                     # 리더 선출, 마스터 이탈
    "o.e.c.c.NodeLeftExecutor",
    "o.e.c.c.NodeJoinExecutor",
    "o.e.c.r.a.AllocationService",             # 샤드 할당 (INFO로 기록된다)
    "o.e.c.r.a.DiskThresholdMonitor",          # 디스크 워터마크
    "o.e.c.r.a.d.DiskThresholdDecider",
    "o.e.m.j.JvmGcMonitorService",             # GC overhead
    "o.e.i.b.HierarchyCircuitBreakerService",  # circuit breaker
    "o.e.c.InternalClusterInfoService",
)
# 로거를 좁혔으므로 300은 과하다. 사고 중 비용 천장을 낮게 유지한다.
_MASTER_LOG_MAX_LINES = 80


# 유입 시작 앞으로 얼마나 더 보는가. 원인은 사고 구간이 아니라 그 앞에서
# 만들어지는 경우가 많다 — heap이 서서히 오르거나 샤드 재배치가 앞서 시작된
# 경우, 유입 구간만 보면 시작점을 통째로 놓친다. 예전 기본값은 1~2분이었고
# 그것은 운영자가 실제로 보는 범위보다 좁았다.
#
# 확장 비용은 유계다. fetch_logs가 구간을 분 단위로 쪼개 조회하고 분·소스마다
# LIMIT이 걸려 있으므로, 5분은 분 세그먼트 5개가 늘어나는 것에 지나지 않는다.
# 조용한 구간의 slowlog·쿼리 로그는 그 상한에 한참 못 미친다(slowlog는 임계
# 초과 쿼리만 기록되므로, 그 구간이 조용했다는 것이 사고가 아니었다는 뜻이다).
#
# 이 값은 프롬프트(deepagent/prompts.py 3단계 "유입 시작 5분 전", 4단계 "5분 더
# 당겨")에도 숫자로 적혀 있다. 고칠 때 함께 고쳐야 한다 — 프롬프트를 f-string으로
# 만들면 본문의 중괄호까지 서식으로 해석되므로 문자열 보간을 쓰지 않았다.
_LOOKBACK_MINUTES = 5

# 유입이 멎었다고 보는 연속 0건 횟수. 1로는 부족하다 — 커넥터 폴링 주기 때문에
# 유입이 계속되는 중에도 한 번은 0건이 나올 수 있다.
_SETTLED_ZERO_STREAK = 2


def _cap_notice() -> str:
    return (
        f"대기 상한 {_MAX_WAIT_SECONDS // 60}분에 도달했다. "
        "더 기다리지 말고 즉시 analyze_logs로 진행하라."
    )


def _suggest_windows(first_seen: datetime, last_seen: datetime) -> list[dict]:
    """관측된 유입을 감싸는 분석 구간을 제안한다.

    **강제가 아니다.** ``analyze_logs``는 여전히 구간을 인자로 받으므로 agent가
    이 제안을 넓히거나 옮길 수 있다. 그것이 필요한 판단이기 때문이다 — 앞
    구간에서 이미 징후가 진행 중이었다면 더 당겨야 하고, 그 판단은 결과를 보고
    나서야 할 수 있다. 코드가 맡는 것은 분할 산수뿐이다.

    구간은 분 경계로 내림·올림한다. 운영자와 모델이 모두 분 단위로 읽고,
    ``fetch_logs``도 분 단위로 쪼개 조회한다.
    """
    start = (first_seen - timedelta(minutes=_LOOKBACK_MINUTES)).replace(
        second=0, microsecond=0
    )
    end = (last_seen + timedelta(minutes=1)).replace(second=0, microsecond=0)
    if end <= start:
        end = start + timedelta(minutes=1)

    windows = []
    cursor = start
    span = timedelta(minutes=_MAX_WINDOW_MINUTES)
    while cursor < end:
        chunk_end = min(cursor + span, end)
        windows.append({"start_iso": _fmt(cursor), "end_iso": _fmt(chunk_end)})
        cursor = chunk_end
    return windows


def _collect_master_logs(
    start_dt: datetime,
    end_dt: datetime,
    *,
    cluster: ClusterRepository,
    fetch_node_logs: Callable[..., list[NodeLogEntry]],
    node_log_fetcher: NodeLogFetcher,
    state: DiagnosisState,
) -> str:
    """구간의 마스터 노드 로그를 모아 synthesis 컨텍스트로 쓸 텍스트를 만든다.

    실패해도 예외를 올리지 않는다 — 이것은 보조 근거이고, 주 분석은 이것 없이도
    성립한다.

    ClickHouse를 먼저 시도하고 **실패일 때만** SSH로 내려간다. 조회가 SSH보다
    나은 이유는 셋이다 — 노드 IP를 얻는 ES 왕복이 없고, severity 정규식 대신
    level 컬럼을 쓰고, 접속 실패라는 실패 갈래 자체가 없다. SSH를 남겨 두는
    것은 적재가 아직 채워지는 중이라서다. 테이블이 안정되면 이 폴백은 지워도
    된다.

    0건에서는 내려가지 않는다. 로거를 좁혀 뒀으므로 건강한 창에서 0건은
    정상이고, 그때마다 SSH로 내려가면 analyze_logs 호출마다(최대 6회) ES 왕복 +
    새 SSH 접속을 치른다. 대가는 적재 지연으로 0건인 경우를 SSH가 메워 주지
    않는 것인데, 0건만 보고는 "사건 없음"과 "적재 안 됨"을 구별할 수 없으므로
    접속 6회 비용이 더 크다고 본다.
    """
    try:
        entries = fetch_node_logs(
            start_dt,
            end_dt,
            node_role=_MASTER_ROLE,
            levels=_MASTER_LOG_LEVELS,
            loggers=_MASTER_EVENT_LOGGERS,
            limit=_MASTER_LOG_MAX_LINES,
        )
        rendered = _render_entries(entries)
        state.record_master_logs(entries)
        _logger.info(
            "[tool] analyze_logs 마스터 로그 %d줄 (ClickHouse)", len(entries)
        )
    except Exception as exc:
        _logger.warning(
            "[tool] analyze_logs 마스터 로그 조회 실패, SSH로 폴백: %s", exc
        )
    else:
        return rendered

    try:
        info = cluster.node_info("_master")
        if not info or not info.get("ip"):
            return ""
        text = node_log_fetcher.fetch(
            info["ip"],
            info["log_path"],
            info["cluster_name"],
            start_dt=start_dt,
            end_dt=end_dt,
        )
    except Exception as exc:
        _logger.warning("[tool] analyze_logs master log 수집 실패: %s", exc)
        return ""

    state.record_master_text(text)
    _logger.info(
        "[tool] analyze_logs 마스터 로그 %d줄 (SSH 폴백)",
        text.count("\n") + 1 if text else 0,
    )
    return text


def make_tools(
    cluster: ClusterRepository,
    fetch_logs: Callable[[TimeRange], list[LogEntry]],
    drain_pending: Callable[[], list[LogEntry]],
    call_llm: LlmCaller,
    call_llm_minute: LlmCaller,
    *,
    node_log_fetcher: NodeLogFetcher,
    fetch_node_logs: Callable[..., list[NodeLogEntry]],
    state: DiagnosisState,
) -> list:
    """tool 묶음을 만든다.

    ``state``는 호출자(``DeepAgentAnalyzer.analyze``)가 소유하고 tool이
    갱신한다. tool은 실패를 예외가 아니라 문자열로 돌려주므로(예외는 agent
    실행 전체를 죽인다) 그것만으로는 호출자가 분석 실패를 알 길이 없다.
    """
    _graph = build_graph(call_llm, call_llm_minute=call_llm_minute)

    # 예산은 클로저에 둔다. DeepAgentAnalyzer.analyze()가 실행마다 make_tools를
    # 새로 부르므로 사고 하나가 끝나면 저절로 0에서 다시 시작한다. 모듈 전역에
    # 두면 첫 사고가 예산을 다 쓰고 이후 사고는 대기를 아예 못 하게 된다.
    analyze_state = {"calls": 0}

    @tool
    def analyze_logs(start_iso: str, end_iso: str) -> str:
        """지정 구간의 로그를 ClickHouse에서 조회해 분 단위로 분석한다.

        최대 분석 윈도우는 10분이다. 초과 시 오류를 반환한다.
        한 번의 진단에서 최대 6회까지만 호출할 수 있다.

        Args:
            start_iso: 구간 시작 시각. ISO 8601 형식. 예) "2026-08-26T02:04:05"
            end_iso: 구간 종료 시각. ISO 8601 형식. 예) "2026-08-26T02:14:05"
        """
        _logger.info("[tool] analyze_logs(%s ~ %s)", start_iso, end_iso)
        if analyze_state["calls"] >= _MAX_ANALYZE_CALLS:
            _logger.warning("[tool] analyze_logs 요청 무시 — 호출 상한 도달")
            # 상한에 걸린 뒤 쓴 리포트는 부분 커버리지다. 거절 문자열만
            # 돌려주고 표식을 남기지 않으면, 구간을 다 못 본 리포트가 완전한
            # 것과 구별되지 않는다.
            return state.mark_gap(
                f"분석 호출 상한({_MAX_ANALYZE_CALLS}회)에 도달했다. "
                f"{start_iso} ~ {end_iso} 구간은 분석하지 못했다. "
                "지금까지의 결과로 리포트를 작성하라."
            )

        start_dt, end_dt, time_error = _parse_window(start_iso, end_iso)
        if time_error:
            return time_error

        if end_dt <= start_dt:
            return "오류: end_iso가 start_iso보다 이전이거나 같다."

        # 같은 구간을 두 번 부르면 상한 6회를 헛되이 태운다. 실측으로
        # 16:09~16:17이 연달아 두 번 요청됐다 — 반환값이 앞선 호출과 같으므로
        # 새로 얻는 것은 없고, 조회와 분 단위 LLM 호출 비용만 그대로 다시 든다.
        # 호출 수를 세기 **전에** 거른다. 거절된 요청은 분석이 아니다.
        if (start_dt, end_dt) in state.analyzed:
            _logger.info("[tool] analyze_logs 중복 요청 — 호출하지 않는다")
            return (
                f"{start_iso} ~ {end_iso} 구간은 이미 분석했다. "
                "앞선 호출의 결과를 그대로 쓰고, 필요하면 다른 구간을 요청하라."
            )
        analyze_state["calls"] += 1

        window_minutes = (end_dt - start_dt).total_seconds() / 60
        if window_minutes > _MAX_WINDOW_MINUTES:
            return (
                f"오류: 요청 윈도우 {window_minutes:.1f}분이 최대({_MAX_WINDOW_MINUTES}분)를 초과한다. "
                f"구간을 좁혀서 다시 호출하라."
            )

        # 커버리지 판정의 입력. 거절된 호출은 기록하지 않는다 — 조회가 일어나지
        # 않았으므로 그 구간을 본 것이 아니다.
        state.requested.append((start_dt, end_dt))

        time_range = TimeRange(start=start_dt, end=end_dt)
        # tool에서 예외가 새어 나가면 agent 실행 전체가 중단된다. 조회·분석
        # 실패는 관찰 결과로 돌려줘 agent가 구간을 좁혀 재시도할 수 있게 한다.
        # make_synthesize의 LLM 호출(nodes.py)은 자체 방어가 없어 여기서 받는다.
        logs: list[LogEntry] = []
        try:
            logs = fetch_logs(time_range)
            if not logs:
                return f"{start_iso} ~ {end_iso} 구간에 로그 없음"

            # 로그를 손에 넣은 **직후에** 거둔다. 노드 메트릭과 후보는 LLM을
            # 전혀 타지 않는 순수 함수가 만드는 값이므로, 이 수집을
            # _graph.invoke 뒤로 미루면 안 된다. 429로 분 단위 호출이 전부
            # 실패하면 synthesize가 예외를 올리고(nodes.py), 그러면 뒤쪽 자리에
            # 도달하지 못해 **조회에 성공한 구간이 리포트에서 통째로 빈칸이
            # 된다** — 운영자에게는 "아무 일도 없던 시간"으로 보인다. 429가 주
            # 실패 모드라는 이 저장소의 전제대로라면 한 구간의 모든 분이 함께
            # 실패하는 것이 가장 흔한 실패 형태다.
            state.record_log_observations(logs)

            # 마스터 노드 로그를 같은 구간으로 수집해 synthesis 컨텍스트에 준다.
            # 실패해도 주 분석을 중단하지 않는다.
            master_logs = _collect_master_logs(
                start_dt,
                end_dt,
                cluster=cluster,
                fetch_node_logs=fetch_node_logs,
                node_log_fetcher=node_log_fetcher,
                state=state,
            )

            state_graph = _graph.invoke(
                {
                    "time_range": time_range,
                    "logs": logs,
                    "buckets": [],
                    "findings": [],
                    "report": "",
                    "master_logs": master_logs,
                },
                config={"max_concurrency": 5},
            )
        except (LlmApiError, LlmResponseError) as exc:
            _logger.warning("[tool] analyze_logs 분석 실패: %s", exc)
            # 실패는 문자열로 돌려주지만, 그 사실은 호출자에게 남긴다.
            # 남기지 않으면 agent가 "분석하지 못했다"는 리포트를 정상 종료로
            # 써 내고 트리거 서비스가 그것을 성공으로 취급한다.
            state.record_failed_timeline(logs)
            return state.mark_window_failed(
                start_dt, end_dt, f"분석 실패({start_iso} ~ {end_iso}): {exc}"
            )
        except Exception as exc:
            _logger.exception("[tool] analyze_logs 조회/분석 오류")
            state.record_failed_timeline(logs)
            return state.mark_window_failed(
                start_dt, end_dt, f"조회/분석 오류({start_iso} ~ {end_iso}): {exc}"
            )

        # 타임라인은 분석이 끝나야 채워진다. 종합 리포트(모델 출력)가 아니라
        # 이쪽이 최종 리포트의 타임라인 섹션이 된다. 분을 키로 덮어쓰는 이유:
        # 실패한 구간을 다시 불러 성공하면 그 분의 failed 표시가 사라져야
        # 한다. 실패 갈래는 record_failed_timeline이 같은 자리를 메운다.
        for finding in state_graph["findings"]:
            if finding.row is not None:
                state.timeline[finding.row.minute] = finding.row

        # 일부 분이 실패한 채 종합된 경우. synthesize는 전 구간이 실패했을
        # 때만 예외를 올리므로 여기까지 왔다면 리포트는 유효하지만 근거가
        # 빠져 있다. 종합 프롬프트에 [분석 실패]로 표기되긴 하나 그것을
        # 리포트에 옮기는 것은 모델 재량이었다.
        failed_minutes = [f for f in state_graph["findings"] if f.failed]
        if failed_minutes:
            state.mark_gap(
                f"{start_iso} ~ {end_iso} 구간 중 {len(failed_minutes)}개 분의 "
                f"분석이 실패했다(전체 {len(state_graph['findings'])}개 분)."
            )

        state.analyzed.append((start_dt, end_dt))
        report = state_graph["report"]
        _logger.info("[tool] analyze_logs 종합 결과:\n%s", report)
        return report + state.candidate_block()

    @tool
    def check_new_slowlogs() -> dict:
        """직전 호출 이후 새로 도착한 slowlog를 확인한다.

        호출할 때마다 큐를 비우므로, 반환값은 '지금까지 쌓인 전량'이 아니라
        '직전 호출 이후 새로 들어온 것'이다. 반복 호출로 유입이 계속되는지
        판단할 수 있다.

        반환: count(건수), earliest(가장 이른 발생 시각), latest(가장 늦은 발생 시각).
        count가 0이면 그 사이 새 slowlog가 없었다는 뜻이다.
        """
        _logger.info("[tool] check_new_slowlogs()")
        entries = drain_pending()

        if entries:
            # 건수와 양 끝 시각만 돌려준다. 유입 판정과 구간 결정에 필요한 것은
            # 그뿐이고, 전량을 돌려주면 유입이 몰릴 때 수백 건이 프롬프트에 실린다.
            times = sorted(e.timestamp for e in entries)
            # slowlog가 미래에 발생할 수는 없다. 노드 시계가 앞서 있으면
            # timestamp가 지금보다 뒤인 값으로 들어오고, 그대로 쓰면 last_seen이
            # 미래가 된다. first_seen은 DiagnosisState가 clock skew를 잡아
            # kafka_receive_time으로 눌러 둔 값이라 기준이 서로 달라지고, 유입
            # 구간이 "수신 시각 ~ 미래"로 벌어진다 — 실측에서 13:14~16:16(3시간)이
            # 되어 커버리지 판정이 "10,200초를 못 봤다"고 말했다.
            now = datetime.now(_KST)
            ahead = [t for t in times if t > now]
            if ahead:
                _logger.warning(
                    "[tool] 발생 시각이 현재보다 미래인 slowlog %d건(최대 %s) — "
                    "clock skew로 보고 현재 시각으로 눌러 쓴다",
                    len(ahead),
                    _fmt(ahead[-1]),
                )
                times = sorted(min(t, now) for t in times)
            state.zero_streak = 0
            # 재트리거로 실행된 경우 기준 시각이 실제 발생보다 늦을 수 있어,
            # 관측된 것이 더 이르면 그쪽으로 당긴다.
            if state.first_seen is None or times[0] < state.first_seen:
                state.first_seen = times[0]
            if state.last_seen is None or times[-1] > state.last_seen:
                state.last_seen = times[-1]
            earliest, latest = _fmt(times[0]), _fmt(times[-1])
        else:
            state.zero_streak += 1
            earliest, latest = None, None

        settled = state.zero_streak >= _SETTLED_ZERO_STREAK
        result = {
            "count": len(entries),
            "earliest": earliest,
            "latest": latest,
            "first_seen": _fmt(state.first_seen) if state.first_seen else None,
            "last_seen": _fmt(state.last_seen) if state.last_seen else None,
            "zero_streak": state.zero_streak,
            "inflow_settled": settled,
            "time_basis": state.time_basis,
        }

        # 제안 구간은 유입이 멎은 뒤에만 싣는다. 2단계 루프에서 이 tool은
        # 여러 번 불리는데, 유입이 진행 중인 동안의 제안은 무의미하면서
        # 토큰은 호출마다 든다.
        if settled and state.first_seen and state.last_seen:
            result["suggested_windows"] = _suggest_windows(
                state.first_seen, state.last_seen
            )

        _logger.info(
            "[tool] check_new_slowlogs → %d건 (first_seen=%s last_seen=%s "
            "zero_streak=%d settled=%s)",
            len(entries),
            result["first_seen"],
            result["last_seen"],
            state.zero_streak,
            settled,
        )
        return result

    @tool
    def cluster_health() -> dict:
        """Elasticsearch 클러스터의 현재 헬스 상태를 반환한다.

        status(green/yellow/red), 활성 샤드 수, 미할당 샤드 수, 노드 수를 포함한다.
        진단 시작 시 항상 먼저 호출해 현재 상태를 파악한다.
        """
        _logger.info("[tool] cluster_health()")
        result = cluster.health()
        # 파싱이 tool 안에서 일어난다. payload가 dict가 아니거나 숫자 칸에
        # 문자열이 오면 int()가 ValueError를 내고, tool에서 샌 예외는 agent
        # 실행 전체를 죽인다 — 상태 이력 한 줄 때문에 진단을 잃지 않는다.
        try:
            state.record_health(result, now=datetime.now(_KST))
            _logger.info("[tool] cluster_health → status=%s", result.get("status"))
        except Exception:
            _logger.exception("[tool] 클러스터 상태 이력 기록 실패")
        return result

    @tool
    def explain_unassigned_shards() -> str:
        """미할당 샤드가 왜 배정되지 못했는지 설명한다.

        cluster_health에서 status가 yellow 또는 red일 때 호출한다.
        할당 문제가 없으면 '미할당 샤드 없음' 메시지를 반환한다.
        """
        _logger.info("[tool] explain_unassigned_shards()")
        try:
            return str(cluster.explain_allocation())
        except Exception as exc:
            # 미할당 샤드가 없으면 ES가 400을 돌려준다. tool에서 예외가 새면
            # agent 실행 전체가 중단되므로 관찰 결과로 바꿔 돌려준다.
            return f"미할당 샤드 없음 또는 조회 불가: {exc}"

    @tool
    def get_node_logs(
        node_id: str,
        start_iso: str,
        end_iso: str,
        keyword: str = "",
        max_lines: int = DEFAULT_HOST_LOG_LINES,
    ) -> str:
        """ES 노드 ID로 해당 노드에 SSH 접속해 지정 구간의 ES 로그를 가져온다.

        내부에서 GET /_nodes/{node_id}로 ip/cluster_name을 조회한 뒤
        SSH로 접속해 severity 키워드(WARN/ERROR/GC/heap/thread pool/reject/shard)
        라인만 필터링한다. node_id 하나로 완결된다.

        분석 결과를 보고 앞 시간대가 더 필요하다고 판단되면 start_iso를 앞으로 당겨
        다시 호출한다. "_master"도 유효한 node_id다.

        Args:
            node_id:   ES 노드 ID. 예) "1xwAA7FOTpekenDXft67Bg", "_master"
            start_iso: 구간 시작. ISO 8601. 예) "2026-09-08T02:00:00"
            end_iso:   구간 종료. ISO 8601. 예) "2026-09-08T02:15:00"
            keyword:   추가 필터 키워드. 비어 있으면 전체. 예) "Exception"
            max_lines: 최대 반환 라인 수 (기본값: 300)
        """
        _logger.info("[tool] get_node_logs(%s, %s ~ %s)", node_id, start_iso, end_iso)
        start_dt, end_dt, time_error = _parse_window(start_iso, end_iso)
        if time_error:
            return time_error

        try:
            info = cluster.node_info(node_id)
        except Exception as exc:
            _logger.warning("[tool] get_node_logs 노드 정보 조회 실패: %s", exc)
            return state.mark_gap(f"노드 정보 조회 실패 (id={node_id}): {exc}")
        if not info:
            return f"노드를 찾지 못함 (id={node_id})"

        ip = info.get("ip", "")
        log_path = info.get("log_path", "")
        cluster_name = info.get("cluster_name", "")
        _logger.info("[tool] get_node_logs → ip=%s cluster_name=%s", ip, cluster_name)

        try:
            result = node_log_fetcher.fetch(
                ip, log_path, cluster_name,
                start_dt=start_dt,
                end_dt=end_dt,
                keyword=keyword,
                max_lines=max_lines,
            )
        except Exception as exc:
            _logger.warning("[tool] get_node_logs SSH 실패: %s", exc)
            return state.mark_gap(f"{node_id} 노드 로그 SSH 수집 실패: {exc}")

        if not result:
            return "(해당 시간대 로그 없음)"
        line_count = result.count("\n") + 1
        _logger.info("[tool] get_node_logs → %d라인", line_count)
        return result

    @tool
    def search_node_logs(
        start_iso: str,
        end_iso: str,
        node: str = "",
        node_role: str = "",
        levels: str = "WARN,ERROR",
        keyword: str = "",
        max_lines: int = DEFAULT_NODE_LOG_LIMIT,
    ) -> str:
        """마스터 노드 로그를 ClickHouse에서 검색한다. SSH 접속이 필요 없다.

        클러스터 차원의 사건(shard 이동, 노드 이탈, 리더 선출, allocation 실패,
        GC, circuit breaker)을 조건으로 걸러 시간순으로 돌려준다. 조건을 비워
        두면 그 조건으로는 걸러내지 않는다.

        **이 테이블에는 마스터 노드 로그만 적재된다.** 데이터 노드 로그는
        여기서 찾으면 0건이므로 get_node_logs(SSH)를 쓴다.

        levels를 지정하면 그 레벨에 더해 클러스터 사건 로거(shard 이동·노드
        이탈·리더 선출·allocation)를 **자동으로 함께** 조회한다. 그 사건들은
        INFO로 기록되므로 levels만으로는 잡히지 않는다. 더 넓게 보려면
        levels=""로 비운다 — 그러면 레벨·로거 조건 없이 전체를 본다.

        구간 길이에 상한이 없다. 사고 전체 구간을 한 번에 요청해도 된다 —
        비용은 max_lines로 묶인다.

        Args:
            start_iso: 구간 시작. ISO 8601, KST 기준. 예) "2026-09-10T02:00:00"
            end_iso:   구간 종료. ISO 8601, KST 기준. 예) "2026-09-10T02:15:00"
            node:      노드 이름. 비우면 전체. 마스터가 여러 대일 때만 쓴다.
            node_role: 노드 역할. 예) "master". 비우면 전체 역할.
            levels:    쉼표로 구분한 로그 레벨. 예) "WARN,ERROR". 비우면 전체 레벨.
            keyword:   line 부분 일치 검색어. 예) "OutOfMemory". 비우면 전체.
            max_lines: 최대 줄 수 (기본 300, 최대 2000).
        """
        _logger.info(
            "[tool] search_node_logs(%s ~ %s, node=%s, role=%s, levels=%s, kw=%s)",
            start_iso, end_iso, node or "-", node_role or "-", levels or "-",
            keyword or "-",
        )
        start_dt, end_dt, time_error = _parse_window(start_iso, end_iso)
        if time_error:
            return time_error

        level_tuple = tuple(part for part in levels.split(",") if part.strip())
        # 레벨을 지정했으면 클러스터 사건 로거를 OR로 함께 넣는다. 프롬프트가
        # 이 tool로 "shard 재배치·노드 이탈·리더 선출·allocation 실패"를 찾으라고
        # 지시하는데 ES는 그것들을 INFO로 남기므로, 기본값 WARN,ERROR만으로는
        # 0건이 돌아온다. 인자로 노출하지 않는 이유는 축약형 로거명을 모델이
        # 정확히 쓸 가능성이 낮고 인자가 하나 더 늘기 때문이다.
        #
        # levels를 비운 것은 "조건을 풀겠다"는 뜻이므로 로거 조건도 함께 뺀다.
        # 여기서 로거를 남기면 비워도 화이트리스트 밖은 못 보게 되어, 프롬프트가
        # 지시하는 "조건을 하나씩 풀어 다시 조회한다"가 성립하지 않는다.
        logger_tuple = _MASTER_EVENT_LOGGERS if level_tuple else ()
        effective_limit = clamp_node_log_limit(max_lines)

        # 조회 실패를 예외로 올리지 않는다. 여기서 예외가 새면 agent 실행
        # 전체가 중단된다. 다만 degraded는 세우지 않는다 — 이 조회는 보조
        # 조사이고, 실패해도 slowlog 분석 결과는 온전하다.
        try:
            entries = fetch_node_logs(
                start_dt,
                end_dt,
                node=node.strip(),
                node_role=node_role.strip(),
                levels=level_tuple,
                loggers=logger_tuple,
                keyword=keyword.strip(),
                limit=effective_limit,
            )
        except InvalidTimeRangeError as exc:
            return f"구간 오류: {exc}"
        except Exception as exc:
            _logger.warning("[tool] search_node_logs 조회 실패: %s", exc)
            return f"노드 로그 조회 실패: {exc}"

        if not entries:
            return (
                f"{start_iso} ~ {end_iso} 구간에 조건에 맞는 노드 로그 없음 "
                f"(node={node or '전체'}, role={node_role or '전체'}, "
                f"levels={levels or '전체'}, keyword={keyword or '없음'}). "
                "조건을 넓혀 다시 조회할 수 있다."
            )

        _logger.info("[tool] search_node_logs → %d줄", len(entries))
        header = (
            f"{start_iso} ~ {end_iso} 노드 로그 {len(entries)}줄 "
            f"(node={node or '전체'}, role={node_role or '전체'}, "
            f"levels={levels or '전체'})"
        )
        # 요청값이 아니라 실제 적용된 상한과 비교한다. 모델이 상한을 넘겨
        # 부르면(자주 그런다) 요청값과 비교하는 순간 절단을 놓친다.
        if len(entries) >= effective_limit:
            header += (
                f" — 상한 {effective_limit}줄에 걸려 가장 이른 쪽만 반환했다. "
                "구간을 좁히거나 조건을 조여서 다시 조회하라."
            )
        body = _render_entries(entries)
        return f"{header}\n{body}"

    @tool
    def sleep(seconds: float) -> str:
        """지정한 초만큼 대기한다. slowlog 유입이 멎기를 기다릴 때 쓴다.

        1회 최대 60초, 한 번의 진단에서 누적 최대 5분까지만 실제로 대기한다.
        누적 상한에 닿으면 더 이상 기다리지 않고 즉시 그 사실을 알린다.

        Args:
            seconds: 대기할 초. 60을 넘기면 60으로 줄여 대기한다.
        """
        if state.wait_seconds >= _MAX_WAIT_SECONDS:
            _logger.info("[tool] sleep 요청 무시 — 대기 상한 도달")
            return _cap_notice()

        requested = float(seconds)
        remaining = _MAX_WAIT_SECONDS - state.wait_seconds
        actual = max(0.0, min(requested, float(_MAX_SLEEP_SECONDS), remaining))

        _logger.info("[tool] sleep(%.0fs, 요청 %.0fs)", actual, requested)
        time.sleep(actual)
        state.wait_seconds += actual
        state.wait_cap_reached = state.wait_seconds >= _MAX_WAIT_SECONDS

        parts = [f"{actual:.0f}초 대기 완료 (누적 {state.wait_seconds:.0f}초)."]
        if actual < requested:
            parts.append(
                f"요청한 {requested:.0f}초는 1회 상한 {_MAX_SLEEP_SECONDS}초로 줄였다."
            )
        if state.wait_cap_reached:
            parts.append(_cap_notice())
        return " ".join(parts)

    return [
        analyze_logs,
        check_new_slowlogs,
        cluster_health,
        explain_unassigned_shards,
        get_node_logs,
        search_node_logs,
        sleep,
    ]
