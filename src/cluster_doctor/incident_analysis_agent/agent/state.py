"""The sole mutable LangGraph state schema owned by the Analysis DeepAgent."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, NotRequired

from deepagents import DeepAgentState

from cluster_doctor.incident_analysis_agent.model.analysis_contract import (
    LogAnalysisRequest,
)
from cluster_doctor.incident_analysis_agent.model.evidence import Evidence
from cluster_doctor.incident_analysis_agent.model.health_point import HealthPoint
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import (
    MasterEvent,
    NodeMetricRow,
    SlowCandidate,
    SourceWindowStatus,
    TimelineRow,
)
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange


def last_write_wins(_current: Any, incoming: Any) -> Any:
    return incoming


class AnalysisAgentState(DeepAgentState):
    """All mutable data for exactly one analysis delegation.

    Collection and observation fields are immutable snapshots. A service may use
    local mutable builders while it runs, but only replacement values return to
    this LangGraph state.
    """

    request: NotRequired[Annotated[LogAnalysisRequest, last_write_wins]]
    collected: NotRequired[Annotated[bool, last_write_wins]]
    execution_failed: NotRequired[Annotated[bool, last_write_wins]]
    evidence: NotRequired[Annotated[tuple[Evidence, ...], last_write_wins]]
    report: NotRequired[Annotated[LogAnalysisReport | None, last_write_wins]]
    report_attempts: NotRequired[Annotated[int, last_write_wins]]
    insufficient_reason: NotRequired[Annotated[str, last_write_wins]]
    suggested_windows: NotRequired[Annotated[tuple[TimeRange, ...], last_write_wins]]
    reanalysis_count: NotRequired[Annotated[int, last_write_wins]]
    evidence_sequence: NotRequired[Annotated[int, last_write_wins]]
    timeline: NotRequired[Annotated[tuple[TimelineRow, ...], last_write_wins]]
    nodes: NotRequired[Annotated[tuple[NodeMetricRow, ...], last_write_wins]]
    master_events: NotRequired[Annotated[tuple[MasterEvent, ...], last_write_wins]]
    health: NotRequired[Annotated[tuple[HealthPoint, ...], last_write_wins]]
    candidates: NotRequired[Annotated[tuple[SlowCandidate, ...], last_write_wins]]
    query_requests: NotRequired[Annotated[tuple[QueryLogEntry, ...], last_write_wins]]
    source_statuses: NotRequired[Annotated[tuple[SourceWindowStatus, ...], last_write_wins]]
    gaps: NotRequired[Annotated[tuple[str, ...], last_write_wins]]
    degraded: NotRequired[Annotated[bool, last_write_wins]]
    time_basis: NotRequired[Annotated[str, last_write_wins]]
    failed_minutes: NotRequired[Annotated[tuple[datetime, ...], last_write_wins]]
    investigated_nodes: NotRequired[Annotated[tuple[str, ...], last_write_wins]]
    unresolved_gaps: NotRequired[Annotated[tuple[TimeRange, ...], last_write_wins]]
    master_log_total: NotRequired[Annotated[int, last_write_wins]]


def initial_analysis_agent_state(
    request: LogAnalysisRequest, *, evidence_sequence: int | None = None
) -> dict[str, object]:
    return {
        "request": request,
        "collected": False,
        "execution_failed": False,
        "evidence": (),
        "report": None,
        "report_attempts": 0,
        "insufficient_reason": "",
        "suggested_windows": (),
        "reanalysis_count": 0,
        "evidence_sequence": request.evidence_sequence_start
        if evidence_sequence is None
        else evidence_sequence,
        "timeline": (),
        "nodes": (),
        "master_events": (),
        "health": (),
        "candidates": (),
        "source_statuses": (),
        "gaps": (),
        "degraded": False,
        "time_basis": "",
        "failed_minutes": (),
        "investigated_nodes": (),
        "unresolved_gaps": (),
        "master_log_total": 0,
    }
