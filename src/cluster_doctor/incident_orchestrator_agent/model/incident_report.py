"""운영자에게 전달되는 관측값, 모델 판단과 구조화된 근거 인용.

관측 수치는 코드가 계산하며 모델 판단과 분리한다. LogAnalysisReport를
projection으로 변환해 HTML과 평문에서 읽는다. 원인 후보별 근거
참조를 유지하며 모델의 수치 재작성이나 문자열 재파싱에 의존하지 않는다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import SuspectPick
from cluster_doctor.incident_orchestrator_agent.model.evidence_citation import (
    EvidenceCitation,
)


@dataclass(frozen=True)
class CauseAssessment:
    statement: str = ""
    supporting: tuple[EvidenceCitation, ...] = ()
    contradicting: tuple[EvidenceCitation, ...] = ()
    mechanism: str = ""
    cause_type: str = "root"


@dataclass(frozen=True)
class Finding:
    """모델이 지목한 관측 사항과 원문 인용.

    ``severity``는 모델의 분류를 보존하며, 코드가 대신 채우지 않는다.
    """

    severity: str
    title: str
    citations: tuple[EvidenceCitation, ...] = ()
    detail: str = ""


@dataclass(frozen=True)
class Recommendation:
    text: str
    citations: tuple[EvidenceCitation, ...] = ()
    cause_index: int | None = None
    wiki_ref: str = ""

    def __str__(self) -> str:
        return self.text


@dataclass(frozen=True)
class Narrative:
    """표시할 관측 사항·원인 판단·후보 선택 이유·조치와 그 근거 인용.

    건수·최대값·시계열 등 코드가 계산한 값은 Observations에서 별도로 읽는다.
    내부 분석의 요약과 미해결 질문은 LogAnalysisReport에 보관한다.
    """

    findings: tuple[Finding, ...] = ()
    suspect_picks: tuple[SuspectPick, ...] = ()
    recommendations: tuple[Recommendation | str, ...] = ()
    causes: tuple[CauseAssessment, ...] = ()


@dataclass(frozen=True)
class TimelineAnnotation:
    """검증 전 모델 타임라인의 구조를 잃지 않고 전달하는 값.

    표시 가능 여부는 최종 리포트의 ``verification_status``와 실제 Evidence
    시각을 함께 보는 projector가 결정한다. 여기서 문자열로 평탄화하면 그
    검증을 다시 수행할 수 없다.
    """

    at: datetime
    description: str
    evidence_refs: tuple[str, ...] = ()


@dataclass(frozen=True)
class IncidentAnalysisReport:
    """운영자에게 전달되는 리포트 한 건.

    ``narrative``는 구조화된 모델 판단이며, 검증을 통과해야 표시된다.
    판단이 없거나 표시할 수 없어도 ``observations``는 그대로 렌더된다.
    """

    observations: Observations
    narrative: Narrative | None = None
    evidence: tuple[Evidence, ...] = ()
    timeline_annotations: tuple[TimelineAnnotation, ...] = ()
    verification_status: str = "NOT_VERIFIED"
    verification_issues: tuple[str, ...] = ()
    cluster: str = ""
    analyzed_from: datetime | None = None
    analyzed_to: datetime | None = None
    title: str = ""
    summary: str = ""
