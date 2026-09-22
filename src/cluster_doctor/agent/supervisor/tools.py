"""Main DeepAgent가 부를 수 있는 Guardrail 도구.

루프를 모델에게 넘기면 "무엇을 분석할지"는 모델이 고른다. 그러나 **고른 것이
허용되는지는 코드가 정한다.** 그 경계가 여기다 — 모델은 구간을 제안할 수 있을
뿐이고, 승인은 ``propose_analysis``를 통과해야만 나온다.

승인된 구간은 반환 문자열이 아니라 graph state(``ADMITTED_WINDOW``)에 쓴다.
문자열로 돌려주면 모델이 그것을 ``task(description=...)``에 옮겨 적는 과정에서
바꿔 쓸 수 있고, 그러면 Guardrail은 통과 의식일 뿐 아무것도 막지 못한다.

**예산은 여기서 차감하지 않는다.** 제안은 공짜고 위임이 비싸다. 승인을 받고도
위임하지 않는 갈래가 있으므로, 분과 호출 수는 실제로 SubAgent를 부르는 순간
``DelegationGuardrailMiddleware``가 예약한다.
"""

from __future__ import annotations

import logging

from langchain.tools import BaseTool, ToolRuntime, tool
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from cluster_doctor.exceptions import GuardrailViolation
from cluster_doctor.storage.incident_state_store import (
    IncidentStateRepository,
)
from cluster_doctor.application.service.guardrails import (
    MAX_ANALYSIS_CALLS,
    MAX_ANALYSIS_WINDOW_MINUTES,
    MAX_ANALYZED_MINUTES,
    MAX_REJECTED_DECISIONS,
    admit_window,
    check_analysis_budget,
    remaining_minutes,
    window_minutes,
)
from cluster_doctor.application.service.window_planner import plan_new_windows
from cluster_doctor.domain.model.incident import IncidentStatus
from cluster_doctor.domain.model.incident_state import IncidentState
from cluster_doctor.contracts.time_range import InvalidTimeRangeError, TimeRange
from cluster_doctor.agent.common.kst import format_kst, parse_kst
from cluster_doctor.agent.supervisor.state import (
    ADMITTED_GOAL,
    ADMITTED_WINDOW,
    DIAGNOSIS_SUBAGENT,
)

_logger = logging.getLogger(__name__)

# ``deepagents``가 붙이는 위임 도구의 이름. 미들웨어가 가로챌 대상을 고르는 데
# 쓰고, 여기서는 모델에게 다음 행동을 일러 주는 문장에 쓴다. 한 곳에만 둔다.
TASK_TOOL_NAME = "task"

_PROPOSE_DESCRIPTION = f"""\
분석하고 싶은 시간 구간을 제안한다. 승인되면 그 구간이 내부 상태에 기록되고,
그 다음에야 {TASK_TOOL_NAME}(subagent_type="{DIAGNOSIS_SUBAGENT}")으로 위임할 수 있다.

- start_kst / end_kst: KST 기준 ISO 8601. 예: 2026-09-18T13:50:00+09:00
- goal: 이 구간을 보려는 이유. 진단 SubAgent가 읽는다.

한 번에 최대 {MAX_ANALYSIS_WINDOW_MINUTES}분. 이미 분석한 구간과 겹치면 겹치지 않는
부분만 승인된다. 승인 결과가 요청과 다를 수 있으므로 반환 문장을 반드시 읽어라.
"""

_FINISH_DESCRIPTION = """\
Incident를 종료한다. 더 볼 구간이 없거나, 더 볼 수 없을 때 부른다.

- outcome: COMPLETED / FAILED / CANCELLED 중 하나.
- reason: 운영자가 읽을 종료 사유. 한 문장.
"""

_CANDIDATES_DESCRIPTION = """\
아직 분석하지 않은 후보 구간과 남은 예산을 돌려준다. 구간을 제안하기 전에 부른다.

- limit: 돌려받을 후보 수(생략 가능).

여기 없는 구간을 제안하면 대개 거절된다. 어떤 구간이 남았는지 직접 계산하지 말고
이 도구를 믿어라.
"""


def make_propose_analysis_tool(
    *, state: IncidentState, repository: IncidentStateRepository
) -> BaseTool:
    """구간 승인 도구를 만든다.

    ``state``와 ``repository``를 인자가 아니라 클로저로 묶는 이유: 도구 인자는
    모델이 채운다. incident_id를 인자로 두면 모델이 남의 Incident 예산을 쓸 수
    있고, 그것은 인자 검증으로 막을 수 있는 종류의 실수가 아니다.
    """

    @tool("propose_analysis", description=_PROPOSE_DESCRIPTION)
    def propose_analysis(
        start_kst: str, end_kst: str, goal: str, runtime: ToolRuntime
    ) -> str | Command:
        try:
            window = TimeRange(start=parse_kst(start_kst), end=parse_kst(end_kst))
        except (InvalidTimeRangeError, ValueError) as exc:
            # 예외를 그대로 올리지 않는다. 도구 오류는 모델에게 "도구가
            # 고장났다"로 보이고, 그러면 같은 인자로 재시도한다. 무엇이 잘못됐고
            # 무엇을 대신 보내야 하는지 문장으로 돌려줘야 다음 시도가 달라진다.
            return _reject(
                state,
                repository,
                reason=f"구간을 읽을 수 없다: {exc}",
                advice=(
                    f"start_kst/end_kst를 KST ISO 8601로 다시 써라. "
                    f"start < end여야 하고 길이는 최대 {MAX_ANALYSIS_WINDOW_MINUTES}분이다. "
                    f"더 넓게 보고 싶으면 {MAX_ANALYSIS_WINDOW_MINUTES}분 이하로 쪼개서 "
                    f"한 조각씩 제안해라."
                ),
            )

        try:
            admitted = admit_window(window, state)
        except GuardrailViolation as exc:
            return _reject(
                state,
                repository,
                reason=str(exc),
                advice=_advice_for(state),
            )

        # 승인이 났으므로 연속 거절 카운터를 되돌린다. 누적으로 세면 앞선 실패
        # 때문에 정상적으로 진행 중인 Incident가 중간에 끊긴다.
        state.rejected_decision_count = 0
        repository.save(state)

        message = _admission_message(requested=window, admitted=admitted, goal=goal)
        _logger.info(
            "[guardrail] 승인 %s~%s (요청 %s~%s)",
            format_kst(admitted.start),
            format_kst(admitted.end),
            format_kst(window.start),
            format_kst(window.end),
        )
        return Command(
            update={
                ADMITTED_WINDOW: {
                    "start": format_kst(admitted.start),
                    "end": format_kst(admitted.end),
                },
                ADMITTED_GOAL: goal.strip(),
                "messages": [ToolMessage(message, tool_call_id=runtime.tool_call_id)],
            }
        )

    return propose_analysis


def make_finish_incident_tool(
    *, state: IncidentState, repository: IncidentStateRepository
) -> BaseTool:
    """종료 도구를 만든다.

    종료를 "모델이 도구를 그만 부르는 것"으로 두면 ``status``와
    ``closing_reason``이 영원히 비고, 운영자는 왜 끝났는지 알 수 없다. 종료도
    상태 전이이므로 자유 텍스트가 아니라 enum으로 받는다.
    """

    @tool("finish_incident", description=_FINISH_DESCRIPTION)
    def finish_incident(outcome: str, reason: str) -> str:
        try:
            status = IncidentStatus(outcome.strip().upper())
        except ValueError:
            return (
                f"outcome={outcome!r}은 알 수 없는 값이다. "
                f"{IncidentStatus.COMPLETED} / {IncidentStatus.FAILED} / "
                f"{IncidentStatus.CANCELLED} 중 하나로 다시 불러라."
            )
        if not status.is_terminal():
            return (
                f"{status}는 종료 상태가 아니다. "
                f"{IncidentStatus.COMPLETED} / {IncidentStatus.FAILED} / "
                f"{IncidentStatus.CANCELLED} 중 하나로 다시 불러라."
            )

        state.status = status
        state.closing_reason = reason.strip()
        repository.save(state)
        _logger.info("[supervisor] Incident 종료 status=%s reason=%s", status, reason.strip())
        return (
            f"Incident를 {status}로 종료했다. 더 이상 도구를 부르지 말고 "
            f"최종 요약만 답해라."
        )

    return finish_incident


def make_list_candidate_windows_tool(*, state: IncidentState) -> BaseTool:
    """남은 후보 구간을 알려 주는 도구를 만든다.

    **"아직 보지 않은 구간"의 차집합 산수를 모델에게 시키지 않는다.**
    ``window_planner``의 모듈 docstring이 이미 그렇게 정해 두었다 — 그것은
    판단이 아니라 계산이고, 계산을 모델이 하면 틀린다.

    이 도구가 없으면 모델은 남은 구간을 산문에서 추측할 수밖에 없다. 그렇게
    나온 제안은 ``propose_analysis``가 거절하고, 거절은 분 예산도 호출 수도
    줄이지 않으므로 사이클 예산만 거절로 태우게 된다. 후보를 코드가 먼저
    내놓으면 그 갈래 자체가 생기지 않는다.

    시각과 개수만 돌려준다. Evidence 원문을 여기 실으면 Context가 사이클마다
    불어나고, ArtifactStore 간접참조가 통째로 무의미해진다.
    """

    @tool("list_candidate_windows", description=_CANDIDATES_DESCRIPTION)
    def list_candidate_windows(limit: int = 4) -> str:
        # ``incident_orchestrator._candidate_windows``와 같은 재료를 쓴다.
        # 제안(pending)과 미해결 구간(unresolved gaps)을 합쳐 이미 분석한 것을
        # 뺀다. 빼는 일은 ``plan_new_windows``가 한다.
        proposed: list[TimeRange] = list(state.pending_windows)
        proposed.extend(state.unresolved_gaps)
        candidates = plan_new_windows(proposed, state, limit=max(1, limit))

        lines: list[str] = []
        if candidates:
            lines.append(f"후보 구간 {len(candidates)}개 (가까운 과거부터):")
            lines.extend(
                f"  {format_kst(window.start)} ~ {format_kst(window.end)} "
                f"({window_minutes(window)}분)"
                for window in candidates
            )
        else:
            lines.append(
                "후보 구간이 없다. 제안된 구간과 미해결 구간이 모두 분석됐다는 뜻이다."
            )
        lines.append(_budget_line(state))
        lines.append(
            "이미 분석한 구간: "
            + (
                ", ".join(
                    f"{format_kst(window.start)}~{format_kst(window.end)}"
                    for window in state.analyzed_windows
                )
                or "없음"
            )
        )
        return "\n".join(lines)

    return list_candidate_windows


def _admission_message(*, requested: TimeRange, admitted: TimeRange, goal: str) -> str:
    """승인 사실을 모델에게 알리는 문장.

    좁혀졌으면 **먼저, 크게** 말한다. 모델이 요청 그대로 승인됐다고 믿으면
    분석되지 않은 구간을 분석됐다고 보고하고, 그 오류는 결과물 어디에서도
    드러나지 않는다.
    """
    lines: list[str] = []
    if admitted != requested:
        lines.append("[주의] 요청한 구간이 그대로 승인되지 않았다.")
        lines.append(
            f"  요청: {format_kst(requested.start)} ~ {format_kst(requested.end)}"
        )
        lines.append(
            f"  승인: {format_kst(admitted.start)} ~ {format_kst(admitted.end)} "
            f"({window_minutes(admitted)}분)"
        )
        lines.append(
            "  이미 분석한 구간과 남은 예산을 뺀 결과다. 승인된 구간만 분석된다 — "
            "나머지가 필요하면 분석이 끝난 뒤 다시 제안해라."
        )
    else:
        lines.append(
            f"승인: {format_kst(admitted.start)} ~ {format_kst(admitted.end)} "
            f"({window_minutes(admitted)}분)"
        )
    if goal.strip():
        lines.append(f"목표: {goal.strip()}")
    lines.append(
        f'이제 {TASK_TOOL_NAME}(subagent_type="{DIAGNOSIS_SUBAGENT}")으로 위임해라. '
        "구간은 이미 내부 상태에 있으므로 description에 시각을 적을 필요가 없고, "
        "적더라도 무시된다."
    )
    return "\n".join(lines)


def _reject(
    state: IncidentState,
    repository: IncidentStateRepository,
    *,
    reason: str,
    advice: str,
) -> str:
    """거절을 기록하고 모델에게 돌려줄 문장을 만든다.

    거절할 때마다 ``rejected_decision_count``를 올린다. 거절된 제안은 분석을
    하지 않았으므로 분 예산도 호출 수도 늘지 않고, 그래서 이 카운터가 없으면
    같은 요청을 무한히 반복하는 갈래에 상한이 존재하지 않는다.
    """
    state.rejected_decision_count += 1
    repository.save(state)
    _logger.warning(
        "[guardrail] 제안 거절 (%d/%d): %s",
        state.rejected_decision_count,
        MAX_REJECTED_DECISIONS,
        reason,
    )

    lines = [f"거절: {reason}", advice]
    if state.rejected_decision_count >= MAX_REJECTED_DECISIONS:
        lines.append(
            f"연속 거절이 상한 {MAX_REJECTED_DECISIONS}회에 도달했다. "
            "같은 제안을 다시 내지 마라 — 지금까지 모은 것으로 결론을 내고 "
            "finish_incident로 종료해라."
        )
    else:
        lines.append(
            f"(연속 거절 {state.rejected_decision_count}/{MAX_REJECTED_DECISIONS})"
        )
    return "\n".join(lines)


def _budget_line(state: IncidentState) -> str:
    """남은 예산 한 줄.

    거절 문장과 후보 목록이 같은 문구를 쓴다. 각자 쓰면 두 도구가 같은 순간에
    다른 잔량을 말하게 되고, 그때 모델이 어느 쪽을 믿을지는 정해져 있지 않다.
    """
    return (
        f"남은 예산: 분석 {remaining_minutes(state)}분 "
        f"(상한 {MAX_ANALYZED_MINUTES}분), "
        f"호출 {max(0, MAX_ANALYSIS_CALLS - state.analysis_call_count)}회 "
        f"(상한 {MAX_ANALYSIS_CALLS}회)."
    )


def _advice_for(state: IncidentState) -> str:
    """거절 뒤 모델이 실제로 고를 수 있는 다음 행동.

    "거절됐다"만 돌려주면 모델은 표현만 바꿔 같은 것을 다시 낸다. 남은 예산과
    이미 본 구간을 함께 줘야 다른 제안이 나온다.
    """
    if remaining_minutes(state) <= 0:
        return (
            f"분석 예산 {MAX_ANALYZED_MINUTES}분을 모두 썼다. 더 제안하지 말고 "
            "지금까지의 근거로 finish_incident를 불러라."
        )
    return (
        f"{_budget_line(state)} "
        "list_candidate_windows로 아직 보지 않은 구간을 확인하고 그중에서 골라라 — "
        "남은 구간을 직접 계산하지 마라. 더 볼 것이 없으면 finish_incident를 불러라."
    )
