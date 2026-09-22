"""위임 경로 위의 Guardrail.

Supervisor의 루프가 Python ``for``에서 모델의 판단으로 옮겨가면서, 상한을
"몇 번 돌았는가"로 셀 수 있는 자리가 사라졌다. 남은 자리는 하나다 — 모델이
실제로 비용을 쓰는 호출, 즉 ``task`` 도구다.

여기서 하는 일은 셋이다.

  1. **우회 차단.** ``task(description=...)``은 모델이 쓴 자유 텍스트다. 거기
     적힌 시각을 믿으면 승인받지 않은 구간이 그대로 분석된다. 그래서 승인된
     구간(``ADMITTED_WINDOW``)이 graph state에 없으면 위임 자체를 막는다.
  2. **선예약.** 예산은 ``handler``를 부르기 **전에** 차감한다. 뒤에 차감하면
     동시에 들어온 ``task`` 둘이 같은 잔량을 보고 둘 다 통과한다. 모델은 한
     턴에 도구 호출을 여러 개 낼 수 있으므로 이것은 가정이 아니라 기본값이다.
  3. **livelock 차단.** 거절은 분도 호출 수도 늘리지 않는다. 그래서 거절만
     반복하는 갈래에는 상한이 없다 — ``MAX_REJECTED_DECISIONS``로 끊는다.

``task`` 외의 도구 호출은 건드리지 않는다.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Any

from langchain.agents.middleware import AgentMiddleware, ToolCallRequest, hook_config
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from cluster_doctor.exceptions import GuardrailViolation
from cluster_doctor.storage.incident_state_store import (
    IncidentStateRepository,
)
from cluster_doctor.incident.guardrails import (
    MAX_ANALYSIS_CALLS,
    MAX_REJECTED_DECISIONS,
    check_analysis_budget,
    check_not_duplicate,
    window_minutes,
)
from cluster_doctor.incident.models import IncidentStatus
from cluster_doctor.incident.state import IncidentState
from cluster_doctor.contracts.time_range import InvalidTimeRangeError, TimeRange
from cluster_doctor.agent.common.kst import format_kst, parse_kst
from cluster_doctor.agent.supervisor.state import (
    ADMITTED_GOAL,
    ADMITTED_WINDOW,
    DIAGNOSIS_SUBAGENT,
)
from cluster_doctor.agent.supervisor.tools import TASK_TOOL_NAME

_logger = logging.getLogger(__name__)


class DelegationGuardrailMiddleware(AgentMiddleware):
    """``task`` 호출 하나하나에 예산과 승인을 강제한다."""

    def __init__(
        self, *, state: IncidentState, repository: IncidentStateRepository
    ) -> None:
        super().__init__()
        self._state = state
        self._repository = repository
        # 검사와 예약 사이를 잠근다. 둘 사이가 열려 있으면 같은 잔량을 본 호출
        # 둘이 모두 통과하고, 그 결과는 예산 초과가 아니라 "예산을 지켰다고
        # 기록된 예산 초과"다 — 사후에 알아챌 방법이 없다.
        self._lock = threading.Lock()
        # state와 별개로 세는 위임 수. ``IncidentState``는 저장소에서 다시
        # 읽히거나 교체될 수 있고, 그때 카운터가 되감기면 상한이 사라진다.
        # 이 프로세스가 실제로 넘긴 위임 수는 여기서만 단조 증가한다.
        self._delegations = 0

    @hook_config(can_jump_to=["end"])
    def after_model(self, state: Any, runtime: Any) -> dict[str, Any] | None:
        """종료 조건이 섰으면 그래프를 **멈춘다.**

        두 가지를 한다.

        1. ``MAX_REJECTED_DECISIONS``를 집행한다. 앞선 구조에서는 이 상한이
           루프를 ``break``했다. 루프가 모델에게 넘어가면서 도구와 미들웨어는
           거절 횟수를 세고 "같은 제안을 다시 내지 마라"고 **적어 보낼** 뿐이
           됐는데, 그것은 강제가 아니다 — 거절은 분도 호출도 먹지 않으므로
           실제 예산 둘 다 이 반복을 잡지 못하고, 남는 것은 recursion limit
           하나뿐이다. 3회에서 끝나던 것이 20턴까지 도는 셈이다.

        2. ``finish_incident``가 이미 상태를 닫았으면 거기서 멈춘다. 닫은 뒤
           모델이 도구를 더 부르는 것은 예산만 쓴다.

        **이 훅이 존재한다는 사실 자체가 세 번째 일을 한다.** after_model
        미들웨어가 하나라도 있어야 langchain이 model 노드의 분기 목적지에
        ``model``을 넣는다. 없으면, 이미 답이 달린 tool call만 남은 턴에서
        분기가 ``model``로 가려다 ``KeyError: 'model'``로 죽는다.
        """
        current = self._repository.get(self._state.incident_id) or self._state

        if current.status.is_terminal():
            return {"jump_to": "end"}

        if current.rejected_decision_count >= MAX_REJECTED_DECISIONS:
            _logger.warning(
                "[guardrail] 연속 거절 %d회 — Incident를 닫는다",
                current.rejected_decision_count,
            )
            # 실패가 아니라 완료다. 거절은 분석이 깨진 것이 아니라 요청이
            # 제약에 걸린 것이고, 그때까지의 분석은 그대로 쓸 수 있다.
            current.status = IncidentStatus.COMPLETED
            current.closing_reason = (
                f"Supervisor의 요청이 연속으로 런타임 제약에 걸려 종료했다 "
                f"({current.rejected_decision_count}회)"
            )
            self._repository.save(current)
            return {"jump_to": "end"}

        return None

    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command[Any]],
    ) -> ToolMessage | Command[Any]:
        if request.tool_call.get("name") != TASK_TOOL_NAME:
            return handler(request)

        tool_call_id = str(request.tool_call.get("id") or "")
        subagent_type = str(request.tool_call.get("args", {}).get("subagent_type") or "")
        if subagent_type != DIAGNOSIS_SUBAGENT:
            # 진단 외의 SubAgent를 허용하면 예산 회계가 깨진다. 승인 하나에
            # 위임 하나라는 규칙은 "그 위임이 승인된 구간을 쓴다"를 전제로 한다.
            return self._reject(
                tool_call_id,
                f'subagent_type="{subagent_type}"에는 위임할 수 없다. '
                f'이 Incident에서 허용된 것은 "{DIAGNOSIS_SUBAGENT}" 하나뿐이다.',
            )

        admitted = _read_state_value(request.state, ADMITTED_WINDOW)
        if not admitted:
            return self._reject(
                tool_call_id,
                "승인된 분석 구간이 없다. 먼저 propose_analysis로 구간을 제안해 "
                "승인을 받아라. description에 시각을 적어도 분석되지 않는다 — "
                "분석 구간은 승인된 것만 쓴다.",
            )

        window = _parse_admitted(admitted)
        if window is None:
            # 승인은 이 프로세스가 직접 썼으므로 여기 오면 상태가 깨진 것이다.
            # 그래도 위임은 막는다 — 읽을 수 없는 구간으로 분석을 시작하는 것이
            # 분석하지 않는 것보다 나쁘다.
            return self._reject(
                tool_call_id,
                f"승인된 구간을 읽을 수 없다({admitted!r}). propose_analysis로 "
                "다시 제안해라.",
            )

        with self._lock:
            if self._delegations >= MAX_ANALYSIS_CALLS:
                return self._reject(
                    tool_call_id,
                    f"위임 상한 {MAX_ANALYSIS_CALLS}회에 도달했다. 더 위임하지 말고 "
                    "지금까지의 근거로 finish_incident를 불러라.",
                )
            try:
                check_analysis_budget(self._state)
                check_not_duplicate(window, self._state)
            except GuardrailViolation as exc:
                return self._reject(tool_call_id, str(exc))

            # **핸들러를 부르기 전에 예약한다.** 분석이 실패해도 되돌리지
            # 않는다 — 실패한 조회도 비용을 썼고, 되돌리면 같은 구간을 무한히
            # 재시도하는 갈래가 열린다.
            self._delegations += 1
            self._state.analysis_call_count += 1
            self._state.analyzed_minutes += window_minutes(window)
            self._state.record_analyzed(window)
            self._state.rejected_decision_count = 0
            self._repository.save(self._state)
            _logger.info(
                "[guardrail] 위임 %d/%d 구간 %s~%s 누적 %d분",
                self._delegations,
                MAX_ANALYSIS_CALLS,
                format_kst(window.start),
                format_kst(window.end),
                self._state.analyzed_minutes,
            )

        # 락 밖에서 부른다. SubAgent 실행은 길고, 그동안 락을 쥐고 있으면 다른
        # 도구 호출까지 멈춘다. 예산은 이미 예약됐으므로 여기서 경쟁은 없다.
        result = handler(request)
        return _consume_admission(result, tool_call_id)

    def _reject(self, tool_call_id: str, reason: str) -> ToolMessage:
        """위임을 막고 이유를 모델에게 돌려준다.

        거절도 ``rejected_decision_count``에 센다. 제안 쪽 거절과 같은 카운터를
        쓰는 이유: 모델이 제안과 위임을 번갈아 실패하면 각각의 카운터는 상한에
        닿지 않은 채 사이클만 탄다.
        """
        self._state.rejected_decision_count += 1
        self._repository.save(self._state)
        _logger.warning(
            "[guardrail] 위임 거절 (%d/%d): %s",
            self._state.rejected_decision_count,
            MAX_REJECTED_DECISIONS,
            reason,
        )
        lines = [f"위임 거절: {reason}"]
        if self._state.rejected_decision_count >= MAX_REJECTED_DECISIONS:
            lines.append(
                f"연속 거절이 상한 {MAX_REJECTED_DECISIONS}회에 도달했다. "
                "같은 호출을 다시 내지 마라 — finish_incident로 종료해라."
            )
        return ToolMessage("\n".join(lines), tool_call_id=tool_call_id, status="error")


def _read_state_value(state: Any, key: str) -> Any:
    """graph state에서 값 하나를 꺼낸다.

    ``ToolCallRequest.state``는 dict일 수도 BaseModel일 수도 있다고 선언돼
    있다. 한쪽만 다루면 state schema가 바뀌는 날 Guardrail이 조용히
    "승인이 없다"로 바뀌거나 — 더 나쁘게는 — 예외로 터진다.
    """
    if state is None:
        return None
    if isinstance(state, dict):
        return state.get(key)
    return getattr(state, key, None)


def _parse_admitted(admitted: Any) -> TimeRange | None:
    """승인된 구간 dict를 ``TimeRange``로. 읽을 수 없으면 ``None``."""
    if not isinstance(admitted, dict):
        return None
    start = admitted.get("start")
    end = admitted.get("end")
    if not isinstance(start, str) or not isinstance(end, str):
        return None
    try:
        return TimeRange(start=parse_kst(start), end=parse_kst(end))
    except (InvalidTimeRangeError, ValueError):
        return None


def _consume_admission(
    result: ToolMessage | Command[Any], tool_call_id: str
) -> ToolMessage | Command[Any]:
    """위임이 끝났으면 승인을 소모한다.

    SubAgent가 ``ADMITTED_WINDOW``를 ``None``으로 되돌리기로 돼 있지만, 그것은
    SubAgent가 정상 종료했을 때의 이야기다. 도중에 죽거나 되돌리는 것을 잊으면
    승인이 남아 다음 ``task``가 그대로 통과한다 — 중복은 ``check_not_duplicate``가
    잡지만, 잡힌 결과는 쓸데없는 거절이다. 여기서 지우면 그 갈래가 없어진다.
    """
    if isinstance(result, Command):
        update = result.update
        if isinstance(update, dict):
            update.setdefault(ADMITTED_WINDOW, None)
            update.setdefault(ADMITTED_GOAL, "")
        return result
    return Command(
        update={
            ADMITTED_WINDOW: None,
            ADMITTED_GOAL: "",
            "messages": [result],
        }
    )
