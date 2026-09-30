"""Reserve Main Agent delegations in state before ToolNode executes them."""

from langchain.agents.middleware import AgentMiddleware, hook_config
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from cluster_doctor.exceptions import GuardrailViolation
from cluster_doctor.incident_analysis_agent.agent.runtime.tool_batch import (
    block_extra_calls,
    pending_calls,
)
from cluster_doctor.incident_analysis_agent.model.kst import parse_kst
from cluster_doctor.incident_analysis_agent.model.time_range import (
    TimeRange,
    is_covered,
)
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisStatus,
    VerificationStatus,
)
from cluster_doctor.incident_orchestrator_agent.agent.state import (
    ADMITTED_GOAL,
    ADMITTED_WINDOW,
    ANALYSIS_SUBAGENT,
)
from cluster_doctor.incident_orchestrator_agent.model.incident import IncidentStatus
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.guardrails import (
    MAX_REJECTED_DECISIONS,
    check_analysis_budget,
    check_not_duplicate,
    remaining_minutes,
    window_minutes,
)

_MUTATING = frozenset({"propose_analysis", "task", "finish_incident"})


class DelegationGuardrailMiddleware(AgentMiddleware):
    @hook_config(can_jump_to=["end"])
    def after_model(self, state, runtime):
        calls = pending_calls(state)
        if (
            state["status"].is_terminal()
            or state.get("rejected_decision_count", 0) >= MAX_REJECTED_DECISIONS
        ):
            status = (
                state["status"]
                if state["status"].is_terminal()
                else (
                    IncidentStatus.COMPLETED
                    if state.get("window_results")
                    else IncidentStatus.FAILED
                )
            )
            return {
                "jump_to": "end",
                "status": status,
                "closing_reason": state.get("closing_reason")
                or "연속 거절 상한에 도달했다",
                "messages": [
                    ToolMessage(
                        "Incident가 종료되어 실행하지 않았다.",
                        tool_call_id=c["id"],
                        name=c["name"],
                        status="error",
                    )
                    for c in calls
                ],
            }
        blocked = block_extra_calls(state, _MUTATING)
        allowed = next((c for c in calls if c["name"] in _MUTATING), None)
        update = {"messages": blocked} if blocked else {}
        if allowed is None or allowed["name"] != "task":
            if blocked:
                update["rejected_decision_count"] = state.get(
                    "rejected_decision_count", 0
                ) + len(blocked)
            return update or None
        try:
            if allowed["args"].get("subagent_type") != ANALYSIS_SUBAGENT:
                raise GuardrailViolation("analysis에만 위임할 수 있다")
            admitted = state.get(ADMITTED_WINDOW)
            if not admitted:
                raise GuardrailViolation(
                    "승인된 분석 구간이 없다. propose_analysis를 먼저 불러라."
                )
            window = TimeRange(
                start=parse_kst(admitted["start"]), end=parse_kst(admitted["end"])
            )
            check_analysis_budget(state)
            check_not_duplicate(window, state)
            if window_minutes(window) > remaining_minutes(state):
                raise GuardrailViolation("승인된 구간이 현재 남은 분 예산을 초과한다")
        except (GuardrailViolation, KeyError, TypeError, ValueError) as exc:
            return {
                **update,
                "rejected_decision_count": state.get("rejected_decision_count", 0)
                + len(blocked)
                + 1,
                "messages": [
                    *blocked,
                    ToolMessage(
                        f"위임 거절: {exc}",
                        tool_call_id=allowed["id"],
                        name="task",
                        status="error",
                    ),
                ],
                "admitted_task_call_id": None,
            }
        analyzed = tuple(state.get("analyzed_windows", ())) + (window,)
        return {
            **update,
            "admitted_task_call_id": allowed["id"],
            "analysis_call_count": state.get("analysis_call_count", 0) + 1,
            "analyzed_minutes": state.get("analyzed_minutes", 0)
            + window_minutes(window),
            "analyzed_windows": analyzed,
            "pending_windows": tuple(
                w
                for w in state.get("pending_windows", ())
                if not is_covered(w, analyzed)
            ),
            "unresolved_gaps": tuple(
                w
                for w in state.get("unresolved_gaps", ())
                if not is_covered(w, analyzed)
            ),
            "rejected_decision_count": len(blocked),
        }

    def wrap_tool_call(self, request, handler):
        if request.tool_call["name"] != "task":
            return handler(request)
        call_id = request.tool_call["id"]
        if request.state.get("admitted_task_call_id") != call_id:
            return ToolMessage(
                "이 호출에는 예약된 승인이 없다.", tool_call_id=call_id, status="error"
            )
        try:
            result = handler(request)
        except Exception as exc:
            return Command(
                update={
                    ADMITTED_WINDOW: None,
                    ADMITTED_GOAL: "",
                    "admitted_task_call_id": None,
                    "latest_analysis_status": LogAnalysisStatus.FAILED,
                    "latest_verification_status": VerificationStatus.NOT_VERIFIED,
                    "accumulated_gaps": tuple(request.state.get("accumulated_gaps", ()))
                    + (f"위임 실행 실패: {type(exc).__name__}",),
                    "messages": [
                        ToolMessage(
                            f"위임 실행 실패: {type(exc).__name__}",
                            tool_call_id=call_id,
                            status="error",
                        )
                    ],
                }
            )
        update = (
            dict(result.update)
            if isinstance(result, Command) and isinstance(result.update, dict)
            else {"messages": [result]}
        )
        return Command(
            update={
                **update,
                ADMITTED_WINDOW: None,
                ADMITTED_GOAL: "",
                "admitted_task_call_id": None,
            }
        )
