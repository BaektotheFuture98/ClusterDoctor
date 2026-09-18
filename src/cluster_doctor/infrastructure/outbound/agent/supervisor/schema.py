"""Supervisor가 채우는 판단 스키마.

``prompt.py``의 ``<decision_output>``이 요구하는 다섯 칸과 정확히 같다. 한쪽만
고치면 모델은 지시대로 쓰는데 파서가 못 읽는 상태가 되고, 그때 증상은 "Supervisor가
늘 종료를 고른다"로만 나타나 원인을 짚기 어렵다.

**모든 필드에 기본값이 있다.** required 필드 하나가 빠지면 구조화 출력 검증이
실패하고, 그 실패는 재시도 루프가 된다. 이 저장소는 429를 최우선 제약으로 다뤄
재시도를 0으로 두는 곳이다.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel, Field, field_validator

from cluster_doctor.domain.model.supervisor_decision import (
    SupervisorAction,
    SupervisorDecision,
)
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.infrastructure.outbound.agent.common.kst import parse_kst

_logger = logging.getLogger(__name__)


class WindowOutput(BaseModel):
    start: str = Field(default="", description="ISO 8601. 예: 2026-09-18T13:50:00+09:00")
    end: str = Field(default="", description="ISO 8601.")


class DecisionOutput(BaseModel):
    action: str = Field(
        default="",
        description=(
            "REQUEST_ANALYSIS / COMPLETE_INCIDENT / FAIL_INCIDENT / CANCEL_INCIDENT "
            "중 하나."
        ),
    )
    analysis_window: WindowOutput | None = Field(
        default=None, description="REQUEST_ANALYSIS일 때만 채운다."
    )
    analysis_goal: str = Field(
        default="", description="이 구간을 추가로 분석하는 이유. 간결하고 구체적으로."
    )
    reason: str = Field(default="", description="선택한 행동의 직접적인 이유만.")
    based_on: list[str] = Field(
        default=[], description="판단의 근거가 된 State 또는 Response 항목 이름."
    )

    @field_validator("action", mode="before")
    @classmethod
    def _normalize_action(cls, value: object) -> str:
        return str(value or "").strip().upper()

    def to_domain(self) -> SupervisorDecision | None:
        """도메인 타입으로 옮긴다. 읽을 수 없으면 ``None``.

        예외를 올리지 않는 이유: 모델의 형식 이탈 하나로 Incident를 죽이지
        않는다. ``None``을 받은 orchestrator는 안전한 쪽 — 종료 — 을 고른다.
        """
        try:
            action = SupervisorAction(self.action)
        except ValueError:
            _logger.warning("[supervisor] 알 수 없는 action=%r", self.action)
            return None

        window = None
        if action is SupervisorAction.REQUEST_ANALYSIS:
            window = self._parse_window()
            if window is None:
                _logger.warning("[supervisor] 분석을 요청했지만 구간을 읽지 못했다")
                return None

        return SupervisorDecision(
            action=action,
            analysis_window=window,
            analysis_goal=self.analysis_goal.strip(),
            reason=self.reason.strip(),
            based_on=tuple(str(item).strip() for item in self.based_on if str(item).strip()),
        )

    def _parse_window(self) -> TimeRange | None:
        if self.analysis_window is None:
            return None
        try:
            return TimeRange(
                start=parse_kst(self.analysis_window.start),
                end=parse_kst(self.analysis_window.end),
            )
        except Exception as exc:
            _logger.warning(
                "[supervisor] 구간을 읽지 못했다 (%r ~ %r): %s",
                self.analysis_window.start,
                self.analysis_window.end,
                exc,
            )
            return None
