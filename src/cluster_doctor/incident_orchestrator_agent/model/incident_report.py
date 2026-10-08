"""운영자에게 전달되는 관측값, 모델 판단과 구조화된 근거 인용.

관측 수치는 코드가 계산하며 모델 판단과 분리한다. LogAnalysisReport를
projection으로 변환해 HTML과 평문에서 읽는다. 원인 후보별 확신도와 근거
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
    confidence: str = ""
    supporting: tuple[EvidenceCitation, ...] = ()
    contradicting: tuple[EvidenceCitation, ...] = ()
    mechanism: str = ""
    uncertainties: tuple[str, ...] = ()


@dataclass(frozen=True)
class Finding:
    """모델이 지목한 문제 하나.

    ``severity``가 필드인 것이 요점이다. 모델이 ``"Critical: …"``처럼 본문에
    섞어 쓰게 두면 notifier가 정규식으로 앞머리를 떼야 하고, 본문에 우연히 들어간
    "Info"까지 배지로 승격된다.

    비어 있을 수 있다. 그때는 렌더러가 "모델이 분류하지 않음"으로 그린다 —
    코드가 대신 채우지 않는다. 이 필드는 **모델의 판단**이다.
    """

    severity: str
    title: str
    evidence: tuple[str, ...] = ()
    citations: tuple[EvidenceCitation, ...] = ()
    detail: str = ""


@dataclass(frozen=True)
class Recommendation:
    text: str
    citations: tuple[EvidenceCitation, ...] = ()
    cause_index: int | None = None

    def __str__(self) -> str:
        return self.text


@dataclass(frozen=True)
class Narrative:
    """모델의 판단.

    관측값은 하나도 담지 않는다. 담으면 모델이 옮겨 적어야 하고, 옮겨 적으면
    틀린다 — 관측값과 최종 출력의 경계를 분리한다.
    """

    headline: str = ""
    context: tuple[str, ...] = ()
    findings: tuple[Finding, ...] = ()
    root_cause: str = ""
    supporting: tuple[str, ...] = ()
    contradicting: tuple[str, ...] = ()
    unverified: tuple[str, ...] = ()
    suspect_picks: tuple[SuspectPick, ...] = ()
    recommendations: tuple[Recommendation | str, ...] = ()
    causes: tuple[CauseAssessment, ...] = ()
    headline_citations: tuple[EvidenceCitation, ...] = ()


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

    ``narrative``와 ``narrative_text``는 배타적이다.

      narrative 있음        구조화 출력 성공. 정상 경로.
      narrative_text 있음   호출자가 평문 참고 자료를 제공했다.
                            렌더러가 평문 자료로 표시한다.
      둘 다 없음            모델이 아무것도 남기지 못했다. 관측값만 그린다.

    셋 중 어느 경우든 ``observations``는 그대로 렌더된다. 그것이 이 설계의
    요점이다 — 모델이 실패해도 운영자는 그 시각에 무슨 일이 있었는지 본다.
    """

    observations: Observations
    narrative: Narrative | None = None
    narrative_text: str = ""
    evidence: tuple[Evidence, ...] = ()
    timeline_annotations: tuple[TimelineAnnotation, ...] = ()
    verification_status: str = "NOT_VERIFIED"
    verification_issues: tuple[str, ...] = ()
    cluster: str = ""
    analyzed_from: datetime | None = None
    analyzed_to: datetime | None = None
