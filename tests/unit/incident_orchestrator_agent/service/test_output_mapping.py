from datetime import datetime, timezone

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisReport,
    ReportFinding,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.output_mapping import (
    to_incident_analysis_report,
)

_UTC = timezone.utc


def test_to_incident_analysis_report_quotes_finding_evidence_with_citation_format():
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
                evidence_refs=("E-abc-1",),
            ),
        ),
    )

    result = to_incident_analysis_report(report, Observations(), [evidence])

    assert result.narrative.findings[0].evidence == (
        "[E-abc-1] | 2026-09-30 15:22:54 | es_query_log | query runtime=28.66s",
    )
