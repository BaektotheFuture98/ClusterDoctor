from datetime import UTC, datetime

from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisReport,
    VerificationStatus,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.output_mapping import (
    to_incident_analysis_report,
)

from cluster_doctor.incident_orchestrator_agent.service.report_delivery.publication.file.html_file_publisher import (
    HtmlFileReportPublisher,
)

_T0 = datetime(2024, 1, 1, 13, 0, tzinfo=UTC)
_T2 = datetime(2024, 1, 1, 13, 20, tzinfo=UTC)


def _report(summary: str, start, end) -> LogAnalysisReport:
    return LogAnalysisReport(
        incident_id="INC-1",
        analyzed_from=start,
        analyzed_to=end,
        summary=summary,
        verification_status=VerificationStatus.PASSED,
    )


async def test_supplied_incident_report_is_projected_and_published_as_html(tmp_path):
    supplied = _report("incident summary across windows", _T0, _T2)
    rendered = to_incident_analysis_report(supplied, Observations(), [])

    publication = await HtmlFileReportPublisher(output_dir=tmp_path).publish(rendered)

    files = list(tmp_path.glob("*.html"))
    assert len(files) == 1
    assert publication.path == files[0]
    assert publication.text_length > 0
    content = files[0].read_text(encoding="utf-8")
    assert content.startswith("<!doctype html>")
    assert "incident summary across windows" in content
