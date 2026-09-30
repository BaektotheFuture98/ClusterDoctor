"""Main Agent tools; all execution data is read from ``ToolRuntime.state``."""

from __future__ import annotations

from collections.abc import Mapping

from langchain.tools import BaseTool, ToolRuntime, tool
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from cluster_doctor.exceptions import GuardrailViolation
from cluster_doctor.incident_analysis_agent.model.kst import format_kst, parse_kst
from cluster_doctor.incident_analysis_agent.model.time_range import (
    InvalidTimeRangeError,
    TimeRange,
)
from cluster_doctor.incident_orchestrator_agent.agent.state import (
    ADMITTED_GOAL,
    ADMITTED_WINDOW,
    ANALYSIS_SUBAGENT,
)
from cluster_doctor.incident_orchestrator_agent.model.incident import IncidentStatus
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.guardrails import (
    MAX_ANALYSIS_CALLS,
    MAX_ANALYZED_MINUTES,
    MAX_REJECTED_DECISIONS,
    admit_window,
    remaining_minutes,
    window_minutes,
)
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.window_planner import (
    plan_new_windows,
)

TASK_TOOL_NAME = "task"


def _state(runtime: ToolRuntime) -> Mapping[str, object]:
    state = runtime.state
    return state if isinstance(state, Mapping) else vars(state)


def make_propose_analysis_tool() -> BaseTool:
    @tool("propose_analysis")
    def propose_analysis(
        start_kst: str, end_kst: str, goal: str, runtime: ToolRuntime
    ) -> Command:
        """Validate and admit one analysis window."""
        state = _state(runtime)
        try:
            requested = TimeRange(start=parse_kst(start_kst), end=parse_kst(end_kst))
            admitted = admit_window(requested, state)
        except (InvalidTimeRangeError, ValueError, GuardrailViolation) as exc:
            rejects = int(state.get("rejected_decision_count", 0)) + 1
            suffix = (
                " finish_incident로 종료해라."
                if rejects >= MAX_REJECTED_DECISIONS
                else " list_candidate_windows를 확인해라."
            )
            return Command(
                update={
                    "rejected_decision_count": rejects,
                    "messages": [
                        ToolMessage(
                            f"거절: {exc}.{suffix}", tool_call_id=runtime.tool_call_id
                        )
                    ],
                }
            )
        message = (
            f"승인: {format_kst(admitted.start)} ~ {format_kst(admitted.end)} "
            f'({window_minutes(admitted)}분). 이제 task(subagent_type="{ANALYSIS_SUBAGENT}")으로 위임해라.'
        )
        return Command(
            update={
                ADMITTED_WINDOW: {
                    "start": format_kst(admitted.start),
                    "end": format_kst(admitted.end),
                },
                ADMITTED_GOAL: goal.strip(),
                "rejected_decision_count": 0,
                "messages": [ToolMessage(message, tool_call_id=runtime.tool_call_id)],
            }
        )

    return propose_analysis


def make_finish_incident_tool() -> BaseTool:
    @tool("finish_incident")
    def finish_incident(outcome: str, reason: str, runtime: ToolRuntime) -> Command:
        """Finish the incident with a terminal outcome."""
        state = _state(runtime)
        try:
            status = IncidentStatus(outcome.strip().upper())
        except ValueError:
            return Command(
                update={
                    "messages": [
                        ToolMessage(
                            "COMPLETED / FAILED / CANCELLED 중 하나여야 한다.",
                            tool_call_id=runtime.tool_call_id,
                        )
                    ]
                }
            )
        if not status.is_terminal() or (
            status is IncidentStatus.COMPLETED and not state.get("window_results")
        ):
            return Command(
                update={
                    "messages": [
                        ToolMessage(
                            "완료에는 구간별 리포트가 하나 이상 필요하다.",
                            tool_call_id=runtime.tool_call_id,
                        )
                    ]
                }
            )
        return Command(
            update={
                "status": status,
                "closing_reason": reason.strip(),
                "messages": [
                    ToolMessage(
                        f"Incident를 {status}로 종료했다.",
                        tool_call_id=runtime.tool_call_id,
                    )
                ],
            }
        )

    return finish_incident


def make_list_candidate_windows_tool() -> BaseTool:
    @tool("list_candidate_windows")
    def list_candidate_windows(limit: int = 4, runtime: ToolRuntime = None) -> str:
        """List windows the Main Agent may still analyze."""
        state = _state(runtime)
        proposed = [
            *state.get("pending_windows", ()),
            *state.get("unresolved_gaps", ()),
        ]
        candidates = plan_new_windows(proposed, state, limit=max(1, limit))
        lines = [
            *(
                f"  {format_kst(w.start)} ~ {format_kst(w.end)} ({window_minutes(w)}분)"
                for w in candidates
            ),
            f"남은 예산: 분석 {remaining_minutes(state)}분 (상한 {MAX_ANALYZED_MINUTES}분), 호출 {max(0, MAX_ANALYSIS_CALLS - int(state.get('analysis_call_count', 0)))}회.",
        ]
        if state.get("observed_end") is not None:
            lines.insert(0, f"유입이 멎은 시각: {format_kst(state['observed_end'])}")
        return "\n".join(lines)

    return list_candidate_windows
