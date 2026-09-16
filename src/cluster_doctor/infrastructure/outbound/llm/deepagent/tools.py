"""DeepAgent tool 정의.

make_tools(cluster, fetch_logs, drain_pending, call_llm, call_llm_minute, run_state=...)
팩토리로 의존성을 클로저에 포획한다. ES 조회는 ClusterRepository 포트를 거친다 —
tool은 elasticsearch 클라이언트를 모른다. run_state는 호출자가 소유하는 dict로,
tool이 실패를 문자열로 삼킬 때 그 사실을 호출자에게 남기는 통로다.

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
import re
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timezone, timedelta

from langchain_core.tools import tool

_KST = timezone(timedelta(hours=9))

# ES 로그 한 줄의 머리: [시각][레벨][로거]. SSH 폴백은 파일 원문을 그대로
# 받으므로 여기서 뽑지 않으면 레벨과 로거가 리포트에 도달하지 못한다.
# 로거 이름은 오른쪽이 공백으로 채워져 있다(``[o.e.c.c.C          ]``).
_ES_LOG_LINE_RE = re.compile(
    r"^\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})[,.]\d+\]"
    r"\[([A-Z ]+)\]"
    r"\[([^\]]+)\]"
)

_logger = logging.getLogger(__name__)

from cluster_doctor.application.port.outbound.cluster_repository import ClusterRepository
from cluster_doctor.application.port.outbound.log_repository import (
    DEFAULT_NODE_LOG_LIMIT,
    clamp_node_log_limit,
)
from cluster_doctor.domain.model.diagnosis_report import HealthPoint, MasterEvent
from cluster_doctor.domain.model.log_entry import LogEntry, NodeLogEntry
from cluster_doctor.domain.model.time_range import (
    MAX_TIME_RANGE_DURATION,
    InvalidTimeRangeError,
    TimeRange,
)
from cluster_doctor.infrastructure.outbound.llm.langgraph.observations import (
    candidate_key,
    merge_node_rows,
    node_metric_summary,
    slow_candidates,
    timeline_row,
)
# 후보 줄은 운영자용 렌더러와 **같은 함수**로 그린다. 여기서 따로 그리면
# 모델이 보는 수치와 리포트에 실리는 수치가 갈리는데, 그 어긋남이 이 변경
# 전체가 없애려던 실패다(es_query_log 264건이 slowlog로 실린 일).
from cluster_doctor.infrastructure.outbound.notifier.report_text import candidate_line
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


def _parse_kst(iso: str) -> datetime:
    """agent가 준 ISO 문자열을 KST-aware datetime으로 만든다.

    ``replace(tzinfo=_KST)``를 쓰면 안 된다. 그것은 변환이 아니라 덮어쓰기라
    ``"...Z"``나 ``"+00:00"``이 붙어 온 순간을 같은 벽시계의 KST로 재해석해
    정확히 9시간 어긋난 구간을 조회한다. 프롬프트가 KST를 지시하더라도
    모델이 지시를 어길 수 있고, 이 오류는 조회가 성공하고 결과만 틀리므로
    어디에서도 드러나지 않는다.
    """
    parsed = datetime.fromisoformat(iso)
    if parsed.utcoffset() is None:
        return parsed.replace(tzinfo=_KST)
    return parsed.astimezone(_KST)


def _parse_window(start_iso: str, end_iso: str):
    """두 ISO 문자열을 KST 구간으로 만든다. ``(start, end, 오류문자열)``.

    세 tool이 같은 6줄을 각자 들고 있었다. 오류 문구까지 복붙돼 있어서, 한
    곳만 고치면 나머지 둘이 다른 말을 하게 되는 상태였다.

    실패를 예외가 아니라 세 번째 항목으로 돌려주는 이유: 호출자는 어차피
    문자열을 반환해야 한다. tool에서 예외가 새면 agent 실행 전체가 중단되므로
    각 tool이 반드시 잡아야 하는데, 그러면 잡는 코드가 다시 세 벌이 된다.
    """
    try:
        return _parse_kst(start_iso), _parse_kst(end_iso), None
    except ValueError as exc:
        return None, None, f"시각 파싱 오류: {exc}"


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

# analyze_logs 호출 한 번이 소스마다 내놓는 느린 요청 후보 수. 모델은 이
# 목록에서 id로 고르기만 하므로 너무 많으면 고르는 일 자체가 어려워지고,
# 너무 적으면 진짜 원인이 목록 밖으로 밀려난다. 호출 상한이 6회이므로
# 최악의 경우에도 후보는 60건을 넘지 않는다.
_CANDIDATES_PER_CALL = 5


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

# 커버리지 판정의 허용오차. agent는 구간을 분 경계로 반올림하므로 유입 마지막
# 몇십 초가 구간 밖으로 밀려나는 일이 정상적으로 생긴다. 그것까지 누락으로
# 보고하면 배너가 잡음이 되고, 잡음이 된 배너는 읽히지 않는다.
_COVERAGE_TOLERANCE = timedelta(seconds=60)


def _cap_notice() -> str:
    return (
        f"대기 상한 {_MAX_WAIT_SECONDS // 60}분에 도달했다. "
        "더 기다리지 말고 즉시 analyze_logs로 진행하라."
    )


def _base_time(
    log_time: datetime, kafka_receive_time: datetime
) -> tuple[datetime, str]:
    """유입 시작 시각의 기준을 정한다.

    프롬프트가 agent에게 시키던 판단이다(``## 시각 기준`` 절). 비교 두 번이라
    모델이 틀릴 이유는 거의 없지만, ``first_seen``을 코드가 누적하려면 기준
    시각도 코드에 있어야 한다 — 이관의 이유는 정확성이 아니라 그쪽이다.

    돌려주는 문자열은 리포트의 "사용한 시각 기준" 필드로 그대로 간다. 그래야
    그 값이 모델의 주장이 아니라 관측된 사실이 된다.
    """
    if log_time > kafka_receive_time:
        # clock skew. slowlog가 수신보다 미래일 수는 없다.
        return kafka_receive_time, "kafka_receive_time (clock skew)"
    if kafka_receive_time - log_time > timedelta(minutes=30):
        return kafka_receive_time, "kafka_receive_time (파이프라인 지연 30분 초과)"
    return log_time, "slowlog_timestamp"


def _fmt(moment: datetime) -> str:
    """프롬프트로 나가는 시각 표기. analyze_logs가 받는 형식과 같다."""
    return moment.astimezone(_KST).strftime("%Y-%m-%dT%H:%M:%S")


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


def _merge(intervals: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    merged: list[tuple[datetime, datetime]] = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def unresolved_failure(observed: dict) -> str | None:
    """성공으로 덮이지 않은 실패 구간이 남았는지 본다.

    실패한 구간을 나중에 다시 불러 종합까지 성공했다면 진단은 성립한 것이므로
    아무 표식도 남기지 않는다. 실패한 채로 끝난 구간이 있을 때만 진단 실패다.

    ``_merge``를 재사용한다 — 재시도 구간이 원래 구간과 정확히 같지 않고
    더 넓게 또는 쪼개져 들어올 수 있다.
    """
    failed = observed.get("failed") or []
    if not failed:
        return None
    covered = _merge(observed.get("analyzed") or [])
    leftover = [
        (start, end)
        for start, end in failed
        if not any(c0 <= start and end <= c1 for c0, c1 in covered)
    ]
    if not leftover:
        return None
    return ", ".join(f"{_fmt(s)} ~ {_fmt(e)}" for s, e in leftover)


def coverage_gaps(observed: dict) -> list[str]:
    """요청된 구간들의 합집합이 관측된 유입을 감쌌는지 본다.

    **합집합으로만 판정한다.** 호출마다 따로 보면 정당한 분할이 전부 위반으로
    잡힌다 — 20분을 두 조각으로 나누면 첫 조각은 ``end < last_seen``이고 둘째
    조각은 ``start > first_seen``인 것이 당연하다.

    판정 대상은 ``[first_seen, last_seen]``, 즉 **관측된 유입 자체**뿐이다.
    ``_LOOKBACK_MINUTES``만큼의 앞 구간은 기본값이지 계약이 아니므로 그것을
    못 채운 것은 누락으로 보고하지 않는다 — 사고가 데이터 보존 기간 앞쪽에서
    시작한 경우처럼 정당하게 못 채우는 경우가 있고, 그때 배너를 띄우면 없는
    문제를 보고하는 것이 된다.

    ``check_new_slowlogs``를 한 번도 부르지 않았으면 ``first_seen``이 없고,
    그때는 아무것도 주장하지 않는다. 근거 없이 누락을 보고하지 않는다.
    """
    first_seen = observed.get("first_seen")
    last_seen = observed.get("last_seen")
    if first_seen is None or last_seen is None:
        return []
    if last_seen < first_seen:
        return []

    requested = observed.get("requested") or []
    if not requested:
        return [
            f"유입 구간({_fmt(first_seen)} ~ {_fmt(last_seen)})을 분석하지 않았다 "
            "— analyze_logs가 호출되지 않았다."
        ]

    uncovered = timedelta()
    cursor = first_seen
    for start, end in _merge(requested):
        if end <= cursor:
            continue
        if start > cursor:
            uncovered += min(start, last_seen) - cursor
        cursor = max(cursor, end)
        if cursor >= last_seen:
            break
    if cursor < last_seen:
        uncovered += last_seen - cursor

    if uncovered <= _COVERAGE_TOLERANCE:
        return []
    return [
        f"관측된 유입 {_fmt(first_seen)} ~ {_fmt(last_seen)} 중 "
        f"{int(uncovered.total_seconds())}초가 분석 구간에 포함되지 않았다."
    ]


def make_tools(
    cluster: ClusterRepository,
    fetch_logs: Callable[[TimeRange], list[LogEntry]],
    drain_pending: Callable[[], list[LogEntry]],
    call_llm: LlmCaller,
    call_llm_minute: LlmCaller,
    *,
    node_log_fetcher: NodeLogFetcher,
    fetch_node_logs: Callable[..., list[NodeLogEntry]],
    run_state: dict,
    log_time: datetime,
    kafka_receive_time: datetime,
) -> list:
    """tool 묶음을 만든다.

    ``run_state``는 호출자(DeepAgentAnalyzer.analyze)가 소유하고 tool이
    갱신하는 실행 결과 표식이다. tool은 실패를 예외가 아니라 문자열로
    돌려주므로(예외는 agent 실행 전체를 죽인다) 그것만으로는 호출자가
    분석 실패를 알 길이 없다. 여기에 남겨 호출자가 읽는다.
    """
    _graph = build_graph(call_llm, call_llm_minute=call_llm_minute)

    # 키가 없으면 _mark_gap이 KeyError를 내고, tool에서 나온 예외는 agent 실행
    # 전체를 죽인다 — tool이 절대 하지 말아야 하는 일이다. 호출자가 채워 주는
    # 것이 계약이지만, 계약 위반의 대가가 진단 소실이라 여기서 받쳐 둔다.
    run_state.setdefault("degraded", False)
    run_state.setdefault("gaps", [])

    # 코드가 관측한 사실. 모델을 거치지 않고 리포트로 직행한다.
    #
    # 전부 **키 기반 dict**인 것이 요점이다. analyze_logs는 한 진단에서 여러 번
    # 불리고 구간이 겹칠 수 있는데, 리스트에 이어 붙이면 같은 분·같은 노드·같은
    # 로그 줄이 두 번 실린다. health만 리스트인 것은 그것이 시간순 이력이기
    # 때문이고, 대신 직전과 같은 상태면 항목을 늘리지 않고 접는다.
    observations = run_state.setdefault(
        "observations",
        {
            "timeline": {},          # dict[datetime, TimelineRow]
            "nodes": {},             # dict[str, NodeMetricRow]
            "master_logs": {},       # dict[key, MasterEvent]
            "health": [],            # list[HealthPoint]
            "candidates": {},        # dict[내용키, SlowCandidate]
            "wait_seconds": 0.0,
            "wait_cap_reached": False,
        },
    )

    def _record_master_logs(entries: list[NodeLogEntry]) -> None:
        """ClickHouse에서 온 마스터 로그를 기록한다. 중복은 내용으로 거른다.

        렌더된 문자열이 아니라 구조로 담는다. 리포트가 같은 사건끼리 묶어야
        하는데(실측 24줄 중 20줄이 같은 follower_check 타임아웃이었다), 묶으려면
        logger가 값으로 있어야 한다.
        """
        store = observations["master_logs"]
        for entry in entries:
            key = (entry.timestamp, entry.node, entry.line)
            if key not in store:
                store[key] = MasterEvent(
                    timestamp=entry.timestamp,
                    node=entry.node,
                    level=(entry.level or entry.detected_level or "").strip(),
                    logger=(entry.logger or "").strip(),
                    line=entry.line,
                    rendered=format_log_line(entry),
                )

    def _record_master_text(text: str) -> None:
        """SSH 폴백으로 온 마스터 로그. 줄 자체가 키다.

        ES 로그 줄에서 시각·레벨·로거를 뽑는다. 세 칸을 비워 두면 리포트가
        사건별로 묶을 때 쓰는 키(``_event_kind``)가 모든 줄에 대해 같은 값이
        되어 **312줄이 헤더 한 줄 + 본문 한 줄로 붕괴한다**(실측). ClickHouse
        경로였다면 나왔을 근거가 SSH로 내려간 진단에서만 사라지는 셈인데, 적재가
        채워지는 중이라 남겨 둔 폴백에서 리포트가 가장 빈약해지는 것은 뒤집혔다.

        뽑지 못한 줄(스택 트레이스 연속 행 등)은 버리지 않는다. 값이 없다는
        것과 줄이 없다는 것은 다르고, 렌더러가 그런 줄을 따로 다룬다.
        """
        store = observations["master_logs"]
        for line in text.splitlines():
            if not line.strip() or line in store:
                continue
            timestamp = None
            level = ""
            logger_name = ""
            match = _ES_LOG_LINE_RE.match(line)
            if match:
                level = match.group(2).strip()
                logger_name = match.group(3).strip()
                try:
                    timestamp = datetime.fromisoformat(match.group(1)).replace(
                        tzinfo=_KST
                    )
                except ValueError:
                    timestamp = None
            store[line] = MasterEvent(
                timestamp=timestamp,
                level=level,
                logger=logger_name,
                line=line,
                rendered=line,
            )

    def _record_candidates(entries: list[LogEntry]) -> None:
        """느린 요청 후보에 id를 붙여 기록한다.

        id는 발견 순서대로 한 번만 붙는다. 겹친 구간을 다시 조회해 같은 요청이
        또 나와도 새 번호를 주지 않는다 — 모델이 이미 본 id가 가리키는 것이
        중간에 바뀌면 안 된다.
        """
        store = observations["candidates"]
        for candidate in slow_candidates(entries, limit=_CANDIDATES_PER_CALL):
            key = candidate_key(candidate)
            if key in store:
                continue
            store[key] = replace(candidate, candidate_id=f"C{len(store) + 1}")

    def _record_logs_observations(entries: list[LogEntry]) -> None:
        """LLM을 타지 않는 관측값을 거둔다. 여기서 예외가 새면 안 된다.

        ``NodeMetricEntry``·``QueryLogEntry``의 필드가 전부 non-Optional이라
        지금은 안전하지만, 어댑터가 ``None``을 하나 흘리면 ``max(int, None)``이
        ``TypeError``가 된다. 이 함수는 tool 안에서 불리고 tool에서 샌 예외는
        agent 실행 전체를 죽여 리포트까지 사라지게 하므로, 방어 비용이 세
        줄이면 건다.
        """
        try:
            merge_node_rows(observations["nodes"], node_metric_summary(entries))
            _record_candidates(entries)
        except Exception:
            _logger.exception("[tool] 관측값 수집 실패 — 분석은 계속한다")

    def _record_failed_timeline(entries: list[LogEntry]) -> None:
        """분석이 실패한 구간의 타임라인을 실패 표식과 함께 채운다.

        행 자체가 없으면 그 분이 타임라인에서 사라지고, "실패해서 못 봤다"가
        "아무 일도 없었다"로 읽힌다 — ``state.py``가 ``row``를 도입하며 적어 둔
        바로 그 실패이고, ``row.failed`` 배너도 행이 없으면 뜨지 않는다.

        건수와 최대값은 ``logs``만으로 계산되므로 LLM이 실패해도 정확하다.
        이미 성공한 분은 덮지 않는다 — 겹친 구간을 다시 부른 경우다.
        """
        if not entries:
            return
        try:
            grouped: dict[datetime, list[LogEntry]] = {}
            for entry in entries:
                minute = entry.timestamp.replace(second=0, microsecond=0)
                grouped.setdefault(minute, []).append(entry)
            for minute, bucket in grouped.items():
                if minute not in observations["timeline"]:
                    observations["timeline"][minute] = timeline_row(
                        minute, bucket, failed=True
                    )
        except Exception:
            _logger.exception("[tool] 실패 구간 타임라인 기록 실패")

    def _candidate_block() -> str:
        """지금까지 모인 느린 요청 후보를 id와 함께 붙인다.

        프롬프트는 "코드가 [C1] [C2] … 후보로 제시하므로 너는 id와 고른 이유만
        쓴다"고 말하는데, **그 목록을 모델에게 보내는 경로가 없었다.**
        ``_record_candidates``는 ``run_state``에만 쓰고 tool이 돌려주는 것은
        종합 텍스트뿐이라, ``suspect_picks[].candidate_id``는 모델이 지어낸
        값이었고 리포트의 조인(``picks.get``)은 100% 빈 문자열을 돌려줬다 —
        "선정 이유"가 한 번도 렌더된 적이 없다.

        누적분을 매번 다 싣는다. 후보는 호출당 상한이 있고 id가 한 번 붙으면
        바뀌지 않아 분량이 폭주하지 않으며, 마지막 호출의 반환값만 보고
        고르게 두면 앞 구간의 후보가 선택지에서 조용히 빠진다.
        """
        store = observations["candidates"]
        if not store:
            return ""
        ordered = sorted(
            store.values(),
            key=lambda c: (len(c.candidate_id), c.candidate_id),
        )
        lines = "\n".join(candidate_line(c) for c in ordered)
        return (
            "\n\n느린 요청 후보 (코드가 고른 것 — id와 수치를 그대로 쓴다):\n"
            f"{lines}"
        )

    def _record_health(payload: dict) -> None:
        """클러스터 상태를 이력에 접어 넣는다.

        cluster_health는 2단계 대기 루프에서 여러 번 불린다. 호출마다 한 줄을
        쌓으면 같은 green이 열 줄 늘어서고, 그 목록은 "상태 변화를 시간순으로"
        라는 리포트의 요구를 오히려 가린다. 직전과 같으면 until만 늘린다.
        """
        now = datetime.now(_KST)
        status = str(payload.get("status", ""))
        unassigned = int(payload.get("unassigned_shards", 0) or 0)
        history = observations["health"]
        if (
            history
            and history[-1].status == status
            and history[-1].unassigned_shards == unassigned
        ):
            history[-1] = replace(history[-1], until=now)
            return
        history.append(
            HealthPoint(
                at=now,
                until=now,
                status=status,
                unassigned_shards=unassigned,
                active_shards=int(payload.get("active_shards", 0) or 0),
                number_of_nodes=int(payload.get("number_of_nodes", 0) or 0),
            )
        )

    def _mark_window_failed(start_dt, end_dt, observation: str) -> str:
        """이 구간 분석이 실패했음을 기록한다. 진단 실패로 확정하지는 않는다.

        같은 구간을 다시 불러 성공하면 없던 일이 된다. 이 경로의 실패는
        일시적인 경우가 많고(실측: nvidia_nim status=529, provider 과부하),
        그때 agent는 같은 구간을 그대로 재호출한다.

        여기서 곧바로 ``run_state["degraded"]``를 박으면 되돌릴 방법이 없다.
        재시도가 성공해도 리포트에 붉은 "분석 실패" 배너가 붙고 재트리거가
        막힌다 — 실측으로 13:54 실패 후 14:00에 같은 구간을 다시 불러 14:04에
        성공했는데도 ``analysis_failed=True``로 나갔다. 거짓 배너는 배너 전체의
        신뢰를 깎으므로, 확정은 실행이 끝난 뒤 ``unresolved_failure``가 한다.

        ``run_state["degraded"]``는 남겨 둔다. 구간으로 귀속되지 않는 실패를
        호출자가 직접 표시할 통로이고, ``analyze()``가 그것과 미해결 구간을
        OR로 묶는다.
        """
        observed["failed"].append((start_dt, end_dt))
        return observation

    def _mark_gap(observation: str) -> str:
        """근거가 일부 빠졌다는 사실을 남긴다. 리포트는 버리지 않는다.

        ``_mark_degraded``와 나누는 기준은 "진단이 성립했는가"다. 보조 조사가
        실패해도 4단계 분석 결과는 온전하므로 리포트를 버릴 이유가 없다 —
        둘을 같이 다루면 SSH 접속 실패 하나로 완성된 리포트가 폐기되고
        재트리거까지 막힌다.

        그렇다고 조용히 넘길 수도 없다. 모델이 "노드 로그 확인 결과 특이사항
        없음"이라고 쓰면 "못 봤다"와 "봤는데 정상이다"가 구별되지 않는다.
        여기 남긴 것은 notifier가 리포트에 배너로 그리므로, 프롬프트를
        어겼더라도 빠진 사실이 운영자에게 반드시 도달한다.
        """
        run_state["gaps"].append(observation)
        return observation

    # 예산은 클로저에 둔다. DeepAgentAnalyzer.analyze()가 실행마다 make_tools를
    # 새로 부르므로 사고 하나가 끝나면 저절로 0에서 다시 시작한다. 모듈 전역에
    # 두면 첫 사고가 예산을 다 쓰고 이후 사고는 대기를 아예 못 하게 된다.
    wait_state = {"slept": 0.0}
    analyze_state = {"calls": 0}

    # 유입 관측값. 이 누적을 모델에게 맡기면 안 된다 — check_new_slowlogs는
    # 호출분만 돌려주고 누적을 안 하므로, 여러 호출에 걸친 최솟값을 모델이
    # 기억으로 들고 있게 된다. 틀려도 검증이 없고, 구간이 좁아지면 근거가
    # 조용히 사라진다.
    #
    # run_state에 두는 이유: analyze()가 실행이 끝난 뒤 커버리지를 판정해야
    # 하고, run_state가 이미 호출자와 공유되는 통로다.
    base, basis = _base_time(log_time, kafka_receive_time)
    observed = run_state.setdefault(
        "observed",
        {
            "first_seen": base,
            "last_seen": None,
            "zero_streak": 0,
            "time_basis": basis,
            # analyze_logs가 실제로 요청한 (start, end). 커버리지 판정의 입력.
            "requested": [],
            # 종합까지 성공한 구간 / 실패한 구간. 실패가 나중에 성공으로
            # 덮이면 없던 일이 된다(unresolved_failure).
            "analyzed": [],
            "failed": [],
        },
    )

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
            return _mark_gap(
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
        if (start_dt, end_dt) in observed["analyzed"]:
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
        observed["requested"].append((start_dt, end_dt))

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
            _record_logs_observations(logs)

            # 마스터 노드 로그를 같은 구간으로 수집해 synthesis 컨텍스트에 준다.
            # 실패해도 주 분석을 중단하지 않는다.
            #
            # ClickHouse를 먼저 시도하고, 빈 결과나 실패일 때만 SSH로 내려간다.
            # 조회가 SSH보다 나은 이유는 셋이다 — 노드 IP를 얻는 ES 왕복이
            # 없고, severity 정규식 대신 level 컬럼을 쓰고, 접속 실패라는
            # 실패 갈래 자체가 없다. SSH를 남겨 두는 것은 적재가 아직 채워지는
            # 중이라서다. 테이블이 안정되면 이 폴백은 지워도 된다.
            master_logs = ""
            # "0건"과 "조회 실패"를 구별한다. 로거를 좁히고 상한을 80으로
            # 내렸으니 건강한 창에서 0건은 정상이고, 그때마다 SSH로 내려가면
            # analyze_logs 호출마다(최대 6회) ES 왕복 + 새 SSH 접속을 치른다 —
            # 이 전환으로 없앤 실패 갈래를 정상 경로에 다시 들여놓는 셈이다.
            #
            # 대가는 적재 지연으로 0건인 경우를 SSH가 메워 주지 않는 것이다.
            # 0건만 보고는 "사건 없음"과 "적재 안 됨"을 구별할 수 없으므로,
            # 접속 6회 비용이 더 크다고 보고 실패에서만 내려간다.
            master_query_failed = False
            try:
                master_entries = fetch_node_logs(
                    start_dt,
                    end_dt,
                    node_role=_MASTER_ROLE,
                    levels=_MASTER_LOG_LEVELS,
                    loggers=_MASTER_EVENT_LOGGERS,
                    limit=_MASTER_LOG_MAX_LINES,
                )
                master_logs = _render_entries(master_entries)
                _record_master_logs(master_entries)
                _logger.info(
                    "[tool] analyze_logs 마스터 로그 %d줄 (ClickHouse)",
                    len(master_entries),
                )
            except Exception as m_exc:
                master_query_failed = True
                _logger.warning(
                    "[tool] analyze_logs 마스터 로그 조회 실패, SSH로 폴백: %s", m_exc
                )

            if master_query_failed:
                try:
                    m_info = cluster.node_info("_master")
                    if m_info and m_info.get("ip"):
                        master_logs = node_log_fetcher.fetch(
                            m_info["ip"], m_info["log_path"], m_info["cluster_name"],
                            start_dt=start_dt,
                            end_dt=end_dt,
                        )
                        _record_master_text(master_logs)
                        _logger.info(
                            "[tool] analyze_logs 마스터 로그 %d줄 (SSH 폴백)",
                            master_logs.count("\n") + 1 if master_logs else 0,
                        )
                except Exception as m_exc:
                    _logger.warning("[tool] analyze_logs master log 수집 실패: %s", m_exc)

            state = _graph.invoke(
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
            _record_failed_timeline(logs)
            return _mark_window_failed(
                start_dt, end_dt, f"분석 실패({start_iso} ~ {end_iso}): {exc}"
            )
        except Exception as exc:
            _logger.exception("[tool] analyze_logs 조회/분석 오류")
            _record_failed_timeline(logs)
            return _mark_window_failed(
                start_dt, end_dt, f"조회/분석 오류({start_iso} ~ {end_iso}): {exc}"
            )

        # 타임라인은 분석이 끝나야 채워진다. 종합 리포트(모델 출력)가 아니라
        # 이쪽이 최종 리포트의 타임라인 섹션이 된다. 분을 키로 덮어쓰는 이유:
        # 실패한 구간을 다시 불러 성공하면 그 분의 failed 표시가 사라져야
        # 한다. 실패 갈래는 _record_failed_timeline이 같은 자리를 메운다.
        for finding in state["findings"]:
            if finding.row is not None:
                observations["timeline"][finding.row.minute] = finding.row

        # 일부 분이 실패한 채 종합된 경우. synthesize는 전 구간이 실패했을
        # 때만 예외를 올리므로 여기까지 왔다면 리포트는 유효하지만 근거가
        # 빠져 있다. 종합 프롬프트에 [분석 실패]로 표기되긴 하나 그것을
        # 리포트에 옮기는 것은 모델 재량이었다.
        failed_minutes = [f for f in state["findings"] if f.failed]
        if failed_minutes:
            _mark_gap(
                f"{start_iso} ~ {end_iso} 구간 중 {len(failed_minutes)}개 분의 "
                f"분석이 실패했다(전체 {len(state['findings'])}개 분)."
            )

        observed["analyzed"].append((start_dt, end_dt))
        report = state["report"]
        _logger.info("[tool] analyze_logs 종합 결과:\n%s", report)
        return report + _candidate_block()

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
            # 미래가 된다. first_seen은 _base_time이 clock skew를 잡아
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
                    _fmt(max(ahead)),
                )
                times = sorted(min(t, now) for t in times)
            observed["zero_streak"] = 0
            # 재트리거로 실행된 경우 기준 시각이 실제 발생보다 늦을 수 있어,
            # 관측된 것이 더 이르면 그쪽으로 당긴다.
            if observed["first_seen"] is None or times[0] < observed["first_seen"]:
                observed["first_seen"] = times[0]
            if observed["last_seen"] is None or times[-1] > observed["last_seen"]:
                observed["last_seen"] = times[-1]
        else:
            observed["zero_streak"] += 1

        settled = observed["zero_streak"] >= _SETTLED_ZERO_STREAK
        result = {
            "count": len(entries),
            "earliest": _fmt(min(e.timestamp for e in entries)) if entries else None,
            "latest": _fmt(max(e.timestamp for e in entries)) if entries else None,
            "first_seen": _fmt(observed["first_seen"]) if observed["first_seen"] else None,
            "last_seen": _fmt(observed["last_seen"]) if observed["last_seen"] else None,
            "zero_streak": observed["zero_streak"],
            "inflow_settled": settled,
            "time_basis": observed["time_basis"],
        }

        # 제안 구간은 유입이 멎은 뒤에만 싣는다. 2단계 루프에서 이 tool은
        # 여러 번 불리는데, 유입이 진행 중인 동안의 제안은 무의미하면서
        # 토큰은 호출마다 든다.
        if settled and observed["first_seen"] and observed["last_seen"]:
            result["suggested_windows"] = _suggest_windows(
                observed["first_seen"], observed["last_seen"]
            )

        _logger.info(
            "[tool] check_new_slowlogs → %d건 (first_seen=%s last_seen=%s "
            "zero_streak=%d settled=%s)",
            len(entries),
            result["first_seen"],
            result["last_seen"],
            observed["zero_streak"],
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
        # 변경 전에는 cluster.health()만 예외원이었고 반환값을 파싱하지 않았다.
        try:
            _record_health(result)
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
            return _mark_gap(f"노드 정보 조회 실패 (id={node_id}): {exc}")
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
            return _mark_gap(f"{node_id} 노드 로그 SSH 수집 실패: {exc}")

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
        if wait_state["slept"] >= _MAX_WAIT_SECONDS:
            _logger.info("[tool] sleep 요청 무시 — 대기 상한 도달")
            return _cap_notice()

        requested = float(seconds)
        remaining = _MAX_WAIT_SECONDS - wait_state["slept"]
        actual = max(0.0, min(requested, float(_MAX_SLEEP_SECONDS), remaining))

        _logger.info("[tool] sleep(%.0fs, 요청 %.0fs)", actual, requested)
        time.sleep(actual)
        wait_state["slept"] += actual
        # 대기 예산은 클로저에만 있어 밖에서 읽을 길이 없다. 리포트의
        # "총 대기 시간 / 대기 상한 도달 여부"가 그 값이므로 여기서 미러한다.
        observations["wait_seconds"] = wait_state["slept"]
        observations["wait_cap_reached"] = wait_state["slept"] >= _MAX_WAIT_SECONDS

        parts = [f"{actual:.0f}초 대기 완료 (누적 {wait_state['slept']:.0f}초)."]
        if actual < requested:
            parts.append(
                f"요청한 {requested:.0f}초는 1회 상한 {_MAX_SLEEP_SECONDS}초로 줄였다."
            )
        if wait_state["slept"] >= _MAX_WAIT_SECONDS:
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
