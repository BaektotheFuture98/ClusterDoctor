"""Main DeepAgent가 소유하는 유일한 가변 LangGraph state 스키마."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, NotRequired

from deepagents import DeepAgentState

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisStatus,
    VerificationStatus,
)
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_orchestrator_agent.model.incident import IncidentStatus
from cluster_doctor.incident_orchestrator_agent.model.window_result import WindowResult

ADMITTED_WINDOW = "admitted_window"
ADMITTED_GOAL = "admitted_goal"
ANALYSIS_SUBAGENT = "analysis"


def last_write_wins(_current: Any, incoming: Any) -> Any:
    """완전한 불변 값으로 통째 교체하는 LangGraph reducer."""
    return incoming


class MainAgentState(DeepAgentState):
    """The complete state of one Main DeepAgent invocation.

    Tool과 middleware는 ``Command(update=...)`` 값을 반환하며, 같은 lifecycle의
    다른 Python 객체를 보관하거나 변경하지 않는다.
    """

    incident_id: NotRequired[Annotated[str, last_write_wins]]
    observed_end: NotRequired[Annotated[datetime | None, last_write_wins]]
    admitted_window: NotRequired[Annotated[dict[str, str] | None, last_write_wins]]
    admitted_goal: NotRequired[Annotated[str, last_write_wins]]
    analyzed_windows: NotRequired[Annotated[tuple[TimeRange, ...], last_write_wins]]
    pending_windows: NotRequired[Annotated[tuple[TimeRange, ...], last_write_wins]]
    unresolved_gaps: NotRequired[Annotated[tuple[TimeRange, ...], last_write_wins]]
    window_results: NotRequired[Annotated[tuple[WindowResult, ...], last_write_wins]]
    evidence: NotRequired[Annotated[tuple[Evidence, ...], last_write_wins]]
    observations: NotRequired[Annotated[Observations, last_write_wins]]
    evidence_counter: NotRequired[Annotated[int, last_write_wins]]
    accumulated_gaps: NotRequired[Annotated[tuple[str, ...], last_write_wins]]
    latest_analysis_status: NotRequired[
        Annotated[LogAnalysisStatus | None, last_write_wins]
    ]
    latest_verification_status: NotRequired[
        Annotated[VerificationStatus | None, last_write_wins]
    ]
    analysis_call_count: NotRequired[Annotated[int, last_write_wins]]
    analyzed_minutes: NotRequired[Annotated[int, last_write_wins]]
    rejected_decision_count: NotRequired[Annotated[int, last_write_wins]]
    total_wait_seconds: NotRequired[Annotated[float, last_write_wins]]
    status: NotRequired[Annotated[IncidentStatus, last_write_wins]]
    closing_reason: NotRequired[Annotated[str, last_write_wins]]
    admitted_task_call_id: NotRequired[Annotated[str | None, last_write_wins]]


def initial_main_agent_state(
    *,
    incident_id: str,
    observed_end: datetime,
    pending_windows: tuple[TimeRange, ...],
    total_wait_seconds: float,
) -> dict[str, Any]:
    """Main Agent 호출 한 번의 완전한 초기값을 만든다."""
    return {
        "incident_id": incident_id,
        "observed_end": observed_end,
        ADMITTED_WINDOW: None,
        ADMITTED_GOAL: "",
        "analyzed_windows": (),
        "pending_windows": pending_windows,
        "unresolved_gaps": (),
        "window_results": (),
        "evidence": (),
        "observations": Observations(),
        "evidence_counter": 0,
        "accumulated_gaps": (),
        "latest_analysis_status": None,
        "latest_verification_status": None,
        "analysis_call_count": 0,
        "analyzed_minutes": 0,
        "rejected_decision_count": 0,
        "total_wait_seconds": total_wait_seconds,
        "status": IncidentStatus.ANALYZING,
        "closing_reason": "",
        "admitted_task_call_id": None,
    }
