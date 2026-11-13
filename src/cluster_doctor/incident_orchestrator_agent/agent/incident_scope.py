"""incident 범위·세션 맥락을 Main 입력으로 옮긴다.

Main은 incident마다 새로 만들어지고 세션을 기억하지 못한다. 범위·마감 사유와
이전 incident 정보는 kafka_consumer가 ``IncidentAnalysisRequest``에 실어 보내고,
여기서 시작 메시지·첫 후보·관측값으로 바뀐다.
"""

from __future__ import annotations

from dataclasses import replace

from cluster_doctor.incident_analysis_agent.model.kst import format_kst
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.service.report_generation.previous_incident import (
    previous_incident_digest,
)
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentAnalysisRequest,
)
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.window_planner import (
    continuation_windows,
    initial_windows,
)


def scope_lines(request: IncidentAnalysisRequest) -> list[str]:
    """시작 메시지에 덧붙일 incident 범위와 세션 맥락."""
    if request.incident_end is None:
        return []
    lines = [
        (
            f"incident 범위: {format_kst(request.observed_start)} ~ "
            f"{format_kst(request.incident_end)} (마감: {request.close.value})"
        )
    ]
    session = request.session
    if session is None or session.sequence == 1:
        return lines
    lines.append(
        f"세션 {session.session_id}의 {session.sequence}번째 incident다. "
        f"이전 incident {session.previous_incident_id}에서 이어진다."
    )
    if session.previous_windows:
        spans = ", ".join(
            f"{format_kst(w.start)}~{format_kst(w.end)}" for w in session.previous_windows
        )
        lines.append(f"이전 incident가 분석한 구간(다시 분석할 수 없다): {spans}")
    if session.previous_report is not None:
        lines += [
            "이전 incident 보고서 (참고용, 이번 incident의 근거가 아니다):",
            previous_incident_digest(session.previous_report),
        ]
    else:
        lines.append("이전 incident 보고서: 없음(분석 실패 또는 검증 미통과)")
    return lines


def first_windows(request: IncidentAnalysisRequest) -> tuple[TimeRange, ...]:
    """첫 후보 구간. 세션의 두 번째 incident부터는 앞당기지 않는다."""
    session = request.session
    if session is not None and session.sequence > 1 and request.incident_end is not None:
        return tuple(continuation_windows(request.observed_start, request.incident_end))
    return tuple(initial_windows(request.observed_start))


def with_incident_health(
    observations: Observations, request: IncidentAnalysisRequest
) -> Observations:
    """incident 구성 중의 health 이력과 범위를 관측값에 합친다.

    사건 종합 **전에** 불러야 종합과 타임라인이 같은 값을 본다. 이력을 앞에
    두는 이유: 분석 구간마다 조회한 분석 시점 값보다 시각이 이르다.
    """
    if request.incident_end is None:
        return observations
    return replace(
        observations,
        health=request.health_history + observations.health,
        incident_span=(request.observed_start, request.incident_end),
    )
