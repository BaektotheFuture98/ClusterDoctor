"""SubAgent가 한 번의 분석 Scope에서 만든 리포트.

표현은 여기 없다. HTML도 PDF도 이 타입을 읽어 그릴 뿐이고, 그 경계를 지키는
것이 ``notifier``다.

모든 주장이 ``evidence_refs``를 달고 다니는 것이 이 타입의 요점이다. 근거
참조가 비어 있는 주장은 Validator가 "unsupported claim"으로 잡는다 — 검증을
가능하게 하는 것이 구조이지 프롬프트가 아니다.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from cluster_doctor.contracts.observations import SuspectPick


class LogAnalysisStatus(StrEnum):
    """SubAgent가 한 번의 분석을 어떻게 끝냈는가."""

    COMPLETED = "COMPLETED"
    # 현재 요청 범위 밖의 시간이 필요하다. Scope 확장은 Supervisor가 승인한다.
    NEED_MORE_CONTEXT = "NEED_MORE_CONTEXT"
    # 허용된 revision을 다 쓰고도 Evidence와 Report가 맞지 않았다.
    VALIDATION_FAILED = "VALIDATION_FAILED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class VerificationStatus(StrEnum):
    """Report Consistency Validator의 판정."""

    PASSED = "PASSED"
    # 검증은 돌았고 불일치가 남았다. 리포트는 존재한다.
    MISMATCH = "MISMATCH"
    # 검증을 돌리지 못했다. 통과와 구별해야 한다.
    NOT_VERIFIED = "NOT_VERIFIED"


class TimelineEvent(BaseModel):
    """사고 전개의 한 칸."""

    model_config = ConfigDict(frozen=True)

    at: datetime
    description: str
    evidence_refs: tuple[str, ...] = Field(default_factory=tuple)


class ReportFinding(BaseModel):
    """모델이 지목한 문제 하나."""

    model_config = ConfigDict(frozen=True)

    severity: str = ""
    title: str = ""
    detail: str = ""
    evidence_refs: tuple[str, ...] = Field(default_factory=tuple)


class RootCause(BaseModel):
    """가장 유력한 원인 하나와 그것을 흔드는 관찰까지.

    ``counter_evidence_refs``가 필드인 것이 중요하다. 반증을 쓸 자리를 만들어
    두지 않으면 모델은 반증을 쓰지 않는다 — 그러면 근거가 얇은 결론과 두꺼운
    결론이 리포트에서 똑같아 보인다.
    """

    model_config = ConfigDict(frozen=True)

    statement: str = ""
    confidence: str = ""
    supporting_evidence_refs: tuple[str, ...] = Field(default_factory=tuple)
    counter_evidence_refs: tuple[str, ...] = Field(default_factory=tuple)


class LogAnalysisReport(BaseModel):
    """한 analysis window의 결과 전부."""

    model_config = ConfigDict(frozen=True)

    incident_id: str

    analyzed_from: datetime
    analyzed_to: datetime

    summary: str = ""

    timeline: tuple[TimelineEvent, ...] = Field(default_factory=tuple)
    findings: tuple[ReportFinding, ...] = Field(default_factory=tuple)
    root_causes: tuple[RootCause, ...] = Field(default_factory=tuple)

    unresolved_questions: tuple[str, ...] = Field(default_factory=tuple)
    recommendations: tuple[str, ...] = Field(default_factory=tuple)

    # 코드가 고른 느린 요청 후보(C1, C2 …) 중 모델이 지목한 것. **id와 이유만**
    # 담는다 — took·쿼리 원문·노드명은 코드가 id로 조인해 붙인다. 모델이 옮겨
    # 적게 시키면 틀리고, 실제로 틀렸다(``took=미확인``).
    suspect_picks: tuple[SuspectPick, ...] = Field(default_factory=tuple)

    evidence_refs: tuple[str, ...] = Field(default_factory=tuple)

    verification_status: VerificationStatus = VerificationStatus.NOT_VERIFIED
    # Validator가 남긴 불일치. PASSED면 비어 있다.
    verification_issues: tuple[str, ...] = Field(default_factory=tuple)
    revision_count: int = 0

    def cited_refs(self) -> set[str]:
        """리포트 본문이 실제로 인용한 참조 전부.

        ``evidence_refs``(수집한 것)와 다르다. 검증은 이 둘의 차이를 본다.
        """
        cited: set[str] = set()
        for event in self.timeline:
            cited.update(event.evidence_refs)
        for finding in self.findings:
            cited.update(finding.evidence_refs)
        for cause in self.root_causes:
            cited.update(cause.supporting_evidence_refs)
            cited.update(cause.counter_evidence_refs)
        return cited
