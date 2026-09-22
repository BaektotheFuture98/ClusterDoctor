"""Incident 하나가 끝날 때까지 유지되는 업무 상태.

**Agent conversation memory가 아니다.** Supervisor의 Context는 사이클마다
버려도 되지만 이 값은 남아야 한다. 그래서 여기 담는 것은 "다음 사이클에서 다시
판단하려면 반드시 필요한 것"으로 한정한다.

담지 않는 것:
  raw log / minute map 중간 결과 / ToolMessage / SSH 원문 / 이전 대화.
그것들이 필요하면 ``ArtifactStore``에서 참조로 꺼낸다 — Context에 쌓이지
않는다는 것이 요점이다.

``analyzed_windows``가 리스트인 것은 시간 순서가 아니라 **집합**으로 쓰기
때문이다. 중복 판정과 차집합 산수는 ``time_range.subtract_spans``가 한다.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from cluster_doctor.domain.model.incident import IncidentStatus
from cluster_doctor.domain.model.log_analysis import (
    LogAnalysisStatus,
    VerificationStatus,
)
from cluster_doctor.contracts.time_range import TimeRange, is_covered


class IncidentState(BaseModel):
    """Supervisor가 다음 행동을 정할 때 읽는 전부."""

    incident_id: str

    analyzed_windows: list[TimeRange] = Field(default_factory=list)
    pending_windows: list[TimeRange] = Field(default_factory=list)
    unresolved_gaps: list[TimeRange] = Field(default_factory=list)

    evidence_refs: list[str] = Field(default_factory=list)
    # 분석이 확보하지 못한 것을 사람이 읽을 문장으로. 리포트 배너가 이것만
    # 읽는다 — 사이클마다 쌓이므로 여기 두지 않으면 앞선 분석의 누락이
    # 사라지고, 리포트가 갖추지 못한 완결성을 주장하게 된다.
    accumulated_gaps: list[str] = Field(default_factory=list)

    latest_report_ref: str | None = None
    latest_analysis_status: LogAnalysisStatus | None = None
    latest_verification_status: VerificationStatus | None = None
    latest_analysis_summary: str = ""

    analysis_call_count: int = 0
    # 지금까지 분석한 **분 수**. 예산의 단위다 — 비용 동인이 호출이 아니라
    # 분이기 때문이다(비어 있지 않은 분마다 datasource별로 LLM이 돈다).
    analyzed_minutes: int = 0
    # Guardrail이 거절한 횟수. 모델이 같은 요청을 반복하면 이 값이 오른다.
    rejected_decision_count: int = 0
    total_wait_seconds: float = 0.0

    status: IncidentStatus = IncidentStatus.OPEN
    # 종료 사유. FAILED/CANCELLED에서 운영자가 읽을 유일한 설명이다.
    closing_reason: str = ""

    def remaining_of(self, window: TimeRange) -> list[TimeRange]:
        """이 구간에서 아직 보지 않은 부분만."""
        from cluster_doctor.contracts.time_range import subtract_spans

        return subtract_spans(window, self.analyzed_windows)

    def record_analyzed(self, window: TimeRange) -> None:
        """분석이 끝난 구간을 반영하고 대기 목록에서 지운다.

        ``pending_windows``에서 **덮인 것을 전부** 지운다. 완전히 같은 것만
        지우면, 13:50~14:05를 13:50~14:00으로 좁혀 분석했을 때 원래 제안이
        영원히 남아 종료 조건을 막는다.
        """
        self.analyzed_windows.append(window)
        self.pending_windows = [
            pending
            for pending in self.pending_windows
            if not is_covered(pending, self.analyzed_windows)
        ]
        self.unresolved_gaps = [
            gap for gap in self.unresolved_gaps if not is_covered(gap, self.analyzed_windows)
        ]
