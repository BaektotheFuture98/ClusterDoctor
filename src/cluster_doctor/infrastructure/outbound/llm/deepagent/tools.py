"""DeepAgent tool 정의.

make_tools(cluster, fetch_logs, drain_pending, call_llm, call_llm_minute, run_state=...)
팩토리로 의존성을 클로저에 포획한다. ES 조회는 ClusterRepository 포트를 거친다 —
tool은 elasticsearch 클라이언트를 모른다. run_state는 호출자가 소유하는 dict로,
tool이 실패를 문자열로 삼킬 때 그 사실을 호출자에게 남기는 통로다.

- analyze_logs(start_iso, end_iso): agent가 ISO 시각으로 구간 지정 → ClickHouse 조회 → LangGraph 분석 (구간 최대 10분, 진단당 최대 6회)
- check_new_slowlogs(): agent 실행 중 큐에 새로 쌓인 slowlog 확인
- cluster_health / explain_unassigned_shards / get_index_summary: ES 직접 호출
- sleep: 대기
"""
from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import datetime, timezone, timedelta

_KST = timezone(timedelta(hours=9))
from langchain_core.tools import tool

_logger = logging.getLogger(__name__)

from cluster_doctor.application.port.outbound.cluster_repository import ClusterRepository
from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.application.port.outbound.llm_analyzer import (
    LlmApiError,
    LlmResponseError,
)
from cluster_doctor.infrastructure.outbound.llm.langgraph.graph import build_graph
from cluster_doctor.infrastructure.outbound.llm.langgraph.nodes import LlmCaller


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


def _cap_notice() -> str:
    return (
        f"대기 상한 {_MAX_WAIT_SECONDS // 60}분에 도달했다. "
        "더 기다리지 말고 즉시 analyze_logs로 진행하라."
    )


def make_tools(
    cluster: ClusterRepository,
    fetch_logs: Callable[[TimeRange], list[LogEntry]],
    drain_pending: Callable[[], list[LogEntry]],
    call_llm: LlmCaller,
    call_llm_minute: LlmCaller,
    *,
    run_state: dict,
) -> list:
    """tool 묶음을 만든다.

    ``run_state``는 호출자(DeepAgentAnalyzer.analyze)가 소유하고 tool이
    갱신하는 실행 결과 표식이다. tool은 실패를 예외가 아니라 문자열로
    돌려주므로(예외는 agent 실행 전체를 죽인다) 그것만으로는 호출자가
    분석 실패를 알 길이 없다. 여기에 남겨 호출자가 읽는다.
    """
    _graph = build_graph(call_llm, call_llm_minute=call_llm_minute)

    _MAX_WINDOW_MINUTES = 10

    # 예산은 클로저에 둔다. DeepAgentAnalyzer.analyze()가 실행마다 make_tools를
    # 새로 부르므로 사고 하나가 끝나면 저절로 0에서 다시 시작한다. 모듈 전역에
    # 두면 첫 사고가 예산을 다 쓰고 이후 사고는 대기를 아예 못 하게 된다.
    wait_state = {"slept": 0.0}
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
            return (
                f"분석 호출 상한({_MAX_ANALYZE_CALLS}회)에 도달했다. "
                "지금까지의 결과로 리포트를 작성하라."
            )
        analyze_state["calls"] += 1

        try:
            start_dt = _parse_kst(start_iso)
            end_dt = _parse_kst(end_iso)
        except ValueError as exc:
            return f"시각 파싱 오류: {exc}"

        if end_dt <= start_dt:
            return "오류: end_iso가 start_iso보다 이전이거나 같다."

        window_minutes = (end_dt - start_dt).total_seconds() / 60
        if window_minutes > _MAX_WINDOW_MINUTES:
            return (
                f"오류: 요청 윈도우 {window_minutes:.1f}분이 최대({_MAX_WINDOW_MINUTES}분)를 초과한다. "
                f"구간을 좁혀서 다시 호출하라."
            )

        time_range = TimeRange(start=start_dt, end=end_dt)
        # tool에서 예외가 새어 나가면 agent 실행 전체가 중단된다. 조회·분석
        # 실패는 관찰 결과로 돌려줘 agent가 구간을 좁혀 재시도할 수 있게 한다.
        # make_synthesize의 LLM 호출(nodes.py)은 자체 방어가 없어 여기서 받는다.
        try:
            logs = fetch_logs(time_range)
            if not logs:
                return f"{start_iso} ~ {end_iso} 구간에 로그 없음"
            state = _graph.invoke(
                {
                    "time_range": time_range,
                    "logs": logs,
                    "buckets": [],
                    "findings": [],
                    "report": "",
                },
                config={"max_concurrency": 1},
            )
        except (LlmApiError, LlmResponseError) as exc:
            _logger.warning("[tool] analyze_logs 분석 실패: %s", exc)
            # 실패는 문자열로 돌려주지만, 그 사실은 호출자에게 남긴다.
            # 남기지 않으면 agent가 "분석하지 못했다"는 리포트를 정상 종료로
            # 써 내고 트리거 서비스가 그것을 성공으로 취급한다.
            run_state["degraded"] = True
            return f"분석 실패({start_iso} ~ {end_iso}): {exc}"
        except Exception as exc:
            _logger.exception("[tool] analyze_logs 조회/분석 오류")
            run_state["degraded"] = True
            return f"조회/분석 오류({start_iso} ~ {end_iso}): {exc}"

        report = state["report"]
        _logger.info("[tool] analyze_logs 종합 결과:\n%s", report)
        return report

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
        if not entries:
            _logger.info("[tool] check_new_slowlogs → 0건")
            return {"count": 0, "earliest": None, "latest": None}

        # 건수와 양 끝 시각만 돌려준다. 유입 판정과 구간 결정에 필요한 것은
        # 그뿐이고, 전량을 돌려주면 유입이 몰릴 때 수백 건이 프롬프트에 실린다.
        times = sorted(e.timestamp for e in entries)
        _logger.info(
            "[tool] check_new_slowlogs → %d건 (%s ~ %s)",
            len(entries), times[0].isoformat(), times[-1].isoformat(),
        )
        return {
            "count": len(entries),
            "earliest": times[0].isoformat(),
            "latest": times[-1].isoformat(),
        }

    @tool
    def cluster_health() -> dict:
        """Elasticsearch 클러스터의 현재 헬스 상태를 반환한다.

        status(green/yellow/red), 활성 샤드 수, 미할당 샤드 수, 노드 수를 포함한다.
        진단 시작 시 항상 먼저 호출해 현재 상태를 파악한다.
        """
        _logger.info("[tool] cluster_health()")
        result = cluster.health()
        _logger.info("[tool] cluster_health → status=%s", result.get("status"))
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
    def get_index_summary(index_pattern: str) -> list[dict]:
        """특정 인덱스 패턴의 상태 요약을 반환한다.

        slowlog에서 문제가 의심되는 인덱스를 발견했을 때 호출한다.
        반환 항목: 인덱스명, 헬스, 상태, 문서수, 저장 크기, 세그먼트 수.

        Args:
            index_pattern: 조회할 인덱스 패턴. 예) 'my-index-2026.08*', 'logs-*'
        """
        _logger.info("[tool] get_index_summary(%s)", index_pattern)
        return cluster.index_summary(index_pattern)

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
        get_index_summary,
        sleep,
    ]
