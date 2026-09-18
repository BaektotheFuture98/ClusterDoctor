"""Supervisor 판단 한 건의 결과.

내부 추론 전체가 아니라 **검증 가능한 것만** 담는다. ``reason``과 ``based_on``이
따로 있는 이유가 그것이다 — 근거로 든 State 항목이 실제로 그 값이었는지는
코드가 확인할 수 있지만, 산문 한 문단은 확인할 수 없다.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from cluster_doctor.domain.model.time_range import TimeRange


class SupervisorAction(StrEnum):
    REQUEST_ANALYSIS = "REQUEST_ANALYSIS"
    COMPLETE_INCIDENT = "COMPLETE_INCIDENT"
    FAIL_INCIDENT = "FAIL_INCIDENT"
    CANCEL_INCIDENT = "CANCEL_INCIDENT"


class SupervisorDecision(BaseModel):
    """다음에 무엇을 할 것인가.

    ``analysis_window``는 ``REQUEST_ANALYSIS``일 때만 의미가 있다. 다른
    action과 함께 오면 orchestrator가 무시한다 — 여기서 예외를 올리면 모델의
    형식 이탈 하나가 Incident를 통째로 죽인다.
    """

    model_config = ConfigDict(frozen=True)

    action: SupervisorAction

    analysis_window: TimeRange | None = None
    analysis_goal: str = ""

    reason: str = ""
    based_on: tuple[str, ...] = Field(default_factory=tuple)

    def is_request(self) -> bool:
        return self.action is SupervisorAction.REQUEST_ANALYSIS
