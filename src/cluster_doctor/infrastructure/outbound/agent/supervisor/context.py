"""Supervisor에게 보여 줄 상태 스냅샷.

**여기 실리는 것이 Supervisor Context의 전부다.** raw 로그도, minute 중간
결과도, ToolMessage도, 앞선 SubAgent 대화도 없다. 사이클마다 이 스냅샷 하나를
새로 그려 보내므로 Context가 분석 횟수만큼 불어나지 않는다 — Agent Context는
버려도 되고 Incident State는 남는다는 원칙의 코드 표현이 이 모듈이다.

``candidate_windows``가 실리는 것이 요점이다. "아직 보지 않은 구간"은 차집합
산수이고 코드가 이미 계산했다. 모델에게 다시 시키면 틀리고, 틀린 것이 중복
분석이면 가장 비싼 자원을 헛되이 태운다.
"""

from __future__ import annotations

from cluster_doctor.domain.model.incident import Incident
from cluster_doctor.domain.model.incident_state import IncidentState
from cluster_doctor.domain.model.log_analysis import LogAnalysisResponse
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.infrastructure.outbound.agent.common.kst import format_kst


def build_decision_prompt(
    incident: Incident,
    state: IncidentState,
    *,
    last_response: LogAnalysisResponse | None,
    candidate_windows: tuple[TimeRange, ...],
    budget_note: str,
) -> str:
    """이번 사이클의 판단 요청."""
    sections = [
        "<incident>",
        f"incident_id: {incident.incident_id}",
        f"cluster: {incident.cluster}",
        f"trigger_time: {format_kst(incident.trigger_time)} KST (slowlog 자체 기재 시각)",
        f"kafka_receive_time: {format_kst(incident.kafka_receive_time)} KST (수신 시각)",
        f"trigger_type: {incident.trigger_type}",
        "</incident>",
        "",
        "<incident_state>",
        f"analyzed_windows: {_windows(state.analyzed_windows)}",
        f"pending_windows: {_windows(state.pending_windows)}",
        f"unresolved_gaps: {_windows(state.unresolved_gaps)}",
        f"analysis_call_count: {state.analysis_call_count}",
        f"latest_analysis_status: {state.latest_analysis_status or '(아직 없음)'}",
        f"verification_status: {state.latest_verification_status or '(아직 없음)'}",
        f"latest_report_ref: {state.latest_report_ref or '(아직 없음)'}",
        f"evidence 수: {len(state.evidence_refs)}",
        f"incident_status: {state.status}",
        "</incident_state>",
    ]

    if last_response is not None:
        sections += [
            "",
            "<log_analysis_response>",
            f"status: {last_response.status}",
            f"analyzed_window: {_window(last_response.analyzed_window)}",
            f"suggested_windows: {_windows(list(last_response.suggested_windows))}",
            f"unresolved_gaps: {_windows(list(last_response.unresolved_gaps))}",
            f"verification_status: {last_response.verification_status}",
            f"report_ref: {last_response.report_ref or '(없음)'}",
            "analysis_summary:",
            last_response.analysis_summary or "(없음)",
            "</log_analysis_response>",
        ]

    sections += [
        "",
        "<runtime>",
        budget_note,
        "",
        "아직 분석하지 않은 구간 (코드가 analyzed_windows를 빼고 계산했다. "
        "REQUEST_ANALYSIS를 고른다면 이 중에서 고른다):",
        _candidates(candidate_windows),
        "</runtime>",
        "",
        "위 상태를 근거로 다음 행동 하나를 결정하고 구조화된 Decision으로 답하라.",
    ]
    return "\n".join(sections)


def _window(window: TimeRange) -> str:
    return f"{format_kst(window.start)} ~ {format_kst(window.end)}"


def _windows(windows: list[TimeRange]) -> str:
    return ", ".join(_window(item) for item in windows) if windows else "(없음)"


def _candidates(windows: tuple[TimeRange, ...]) -> str:
    if not windows:
        return "(없음 — 새로 분석할 구간이 남아 있지 않다)"
    return "\n".join(f"- {_window(item)}" for item in windows)
