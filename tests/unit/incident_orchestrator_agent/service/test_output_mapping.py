from datetime import UTC, datetime

from cluster_doctor.incident_analysis_agent.model.evidence import (
    Evidence,
    EvidenceSource,
)
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisReport,
    ReportFinding,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.output_mapping import (
    to_incident_analysis_report,
)

_UTC = UTC


def test_finding_citations_preserve_sources_deduplicate_and_expose_missing_refs():
    evidence = Evidence(
        evidence_id="E-abc-1",
        event_time=datetime(2026, 9, 30, 15, 22, 54, tzinfo=_UTC),
        source=EvidenceSource.QUERY_LOG,
        message="query runtime=28.66s",
    )
    report = LogAnalysisReport(
        incident_id="INC-1",
        analyzed_from=datetime(2026, 9, 30, 15, 0, 0, tzinfo=_UTC),
        analyzed_to=datetime(2026, 9, 30, 16, 0, 0, tzinfo=_UTC),
        findings=(
            ReportFinding(
                severity="Warning",
                title="느린 쿼리",
                evidence_refs=("E-abc-1", "MISSING", "E-abc-1"),
                detail="실행시간 확인",
            ),
        ),
    )

    result = to_incident_analysis_report(report, Observations(), [evidence])

    finding = result.narrative.findings[0]
    assert finding.detail == "실행시간 확인"
    assert tuple(c.evidence_id for c in finding.citations) == ("E-abc-1", "MISSING")
    assert finding.citations[0].evidence is evidence
    assert finding.citations[1].evidence is None
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import visible_citations
    shown = visible_citations(finding.citations)
    assert "query runtime=28.66s" in shown[0]
    assert shown[1] == "인용 원문 확인 불가"
