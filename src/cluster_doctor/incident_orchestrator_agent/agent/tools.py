"""Main Agent tool. 실행 데이터는 모두 ``ToolRuntime.state``에서 읽는다."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated

from langchain.tools import BaseTool, ToolRuntime, tool
from langchain_core.messages import ToolMessage
from langgraph.types import Command
from pydantic import BaseModel, Field

from cluster_doctor.exceptions import GuardrailViolation
from cluster_doctor.incident_analysis_agent.model.kst import format_kst, parse_kst
from cluster_doctor.incident_analysis_agent.model.time_range import (
    InvalidTimeRangeError,
    MAX_TIME_RANGE_DURATION,
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

_PROPOSE_DESCRIPTION = f"""분석 구간을 검증하고, 승인된 구간과 목표를 저장한다.
시작은 포함하고 종료는 포함하지 않는다. 한 번에 최대 {int(MAX_TIME_RANGE_DURATION.total_seconds() / 60)}분이다.
부분 중복은 아직 보지 않은 앞쪽 조각으로, 남은 분 예산 초과는 예산에 맞춰 줄일 수 있다.
전부 분석한 구간, 구간 길이 상한 초과, 분·호출 예산 소진, 잘못된 시각은 거절한다.
응답에는 실제 승인 구간 또는 거절 사유가 나온다."""

_TASK_DESCRIPTION = f"""propose_analysis가 승인한 구간 하나를 {ANALYSIS_SUBAGENT}에 위임한다.
승인 하나에 한 번만 호출할 수 있다. 분석 구간과 목표는 승인 기록에서 읽는다.
반환된 JSON의 구간 분석 결과를 읽고 다음 행동을 판단한다."""


class _AnalysisTaskInput(BaseModel):
    description: str = Field(
        description="승인된 goal을 짧게 반복한다. 새 분석 지시를 이 필드에만 추가하지 않는다."
    )
    subagent_type: str = Field(description=f'위임 대상. "{ANALYSIS_SUBAGENT}"를 사용한다.')


def describe_analysis_task(tool: BaseTool) -> BaseTool:
    """DeepAgents의 task 실행은 유지하고 모델에 전달할 설명·입력 schema를 교체한다."""
    return tool.model_copy(
        update={"description": _TASK_DESCRIPTION, "args_schema": _AnalysisTaskInput}
    )


def _state(runtime: ToolRuntime) -> Mapping[str, object]:
    state = runtime.state
    return state if isinstance(state, Mapping) else vars(state)


def make_propose_analysis_tool() -> BaseTool:
    @tool("propose_analysis", description=_PROPOSE_DESCRIPTION)
    def propose_analysis(
        start_kst: Annotated[str, "분석 시작 시각. KST 기준 ISO 8601 문자열."],
        end_kst: Annotated[str, "분석 종료 시각. KST 기준 ISO 8601 문자열."],
        goal: Annotated[
            str, "미확인 사항과 이 시간대가 필요한 이유, 모든 분석 지시를 작성한다."
        ],
        runtime: ToolRuntime,
    ) -> Command:
        """분석 window 하나를 검증하고 수락한다."""
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
    @tool(
        "finish_incident",
        description="Incident를 종료 상태로 닫는다. 응답으로 종료 결과 또는 거절 사유를 반환한다.",
    )
    def finish_incident(
        outcome: Annotated[str, "종료 결과. COMPLETED / FAILED / CANCELLED 중 하나."],
        reason: Annotated[
            str, "운영자에게 전달할 종료 이유와 남은 질문·검증 한계를 간결하게 작성한다."
        ],
        runtime: ToolRuntime,
    ) -> Command:
        """종료 결과(terminal outcome)로 incident를 마무리한다."""
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
    @tool(
        "list_candidate_windows",
        description=(
            "아직 분석하지 않은 후보 구간과 남은 분·호출 예산, 마지막 관측 시각을 반환한다. "
            "추가 구간 제안은 후보 목록에 반영된다."
        ),
    )
    def list_candidate_windows(
        limit: Annotated[int, "최대로 보여줄 후보 구간 수."] = 4,
        runtime: ToolRuntime = None,
    ) -> str:
        """Main Agent가 아직 분석할 수 있는 window 목록을 반환한다."""
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
