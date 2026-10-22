"""분석 리포트와 누적 관측값·근거를 운영자 전달 모델로 변환한다.

주장의 근거 ID는 EvidenceCitation으로 연결하고, 원인·조치의 연결과
타임라인·검증 상태를 보존한다. HTML·평문에 표시하지 않는 내부 분석 요약과
미해결 질문은 전달 모델에 복제하지 않는다. 표시 여부는 렌더링 투영에서 결정한다.
"""

from __future__ import annotations

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport
from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    CauseAssessment,
    Finding,
    IncidentAnalysisReport,
    Narrative,
    Recommendation,
    TimelineAnnotation,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.evidence_citation import (
    citations,
)


def to_incident_analysis_report(
    report: LogAnalysisReport | None,
    observations: Observations,
    evidence: list[Evidence],
    *,
    cluster: str = "",
) -> IncidentAnalysisReport:
    """분석 리포트가 없어도 관측값과 수집 근거를 전달한다."""
    if report is None:
        return IncidentAnalysisReport(
            observations=observations, evidence=tuple(evidence), cluster=cluster
        )

    return IncidentAnalysisReport(
        observations=observations,
        evidence=tuple(evidence),
        title=report.title,
        summary=report.summary,
        future_improvements=report.future_improvements,
        timeline_annotations=tuple(
            TimelineAnnotation(
                at=event.at,
                description=event.description,
                evidence_refs=event.evidence_refs,
            )
            for event in report.timeline
        ),
        verification_status=report.verification_status.value,
        verification_issues=report.verification_issues,
        cluster=cluster,
        analyzed_from=report.analyzed_from,
        analyzed_to=report.analyzed_to,
        narrative=Narrative(
            findings=tuple(
                Finding(
                    severity=item.severity,
                    title=item.title,
                    citations=citations(item.evidence_refs, tuple(evidence)),
                    detail=item.detail,
                )
                for item in report.findings
            ),
            suspect_picks=report.suspect_picks,
            recommendations=tuple(
                Recommendation(
                    text=item.text,
                    cause_index=item.cause_index,
                    wiki_ref=item.wiki_ref,
                    citations=citations(item.evidence_refs, tuple(evidence)),
                )
                for item in report.recommendations
            ),
            causes=tuple(
                CauseAssessment(
                    statement=cause.statement,
                    mechanism=cause.mechanism,
                    cause_type=cause.cause_type,
                    supporting=citations(
                        cause.supporting_evidence_refs, tuple(evidence)
                    ),
                    contradicting=citations(
                        cause.counter_evidence_refs, tuple(evidence)
                    ),
                )
                for cause in report.root_causes
            ),
        ),
    )
