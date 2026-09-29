from datetime import datetime, timezone

from cluster_doctor.incident_analysis_agent.model.basemodel.observations import Observations
from cluster_doctor.incident_analysis_agent.model.basemodel.report import (
    LogAnalysisReport,
    VerificationStatus,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.output_mapping import (
    to_incident_analysis_report,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import (
    HtmlFileReportPublisher,
)

_T0 = datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc)
_T1 = datetime(2024, 1, 1, 13, 10, tzinfo=timezone.utc)
_T2 = datetime(2024, 1, 1, 13, 20, tzinfo=timezone.utc)


def _report(summary: str, start, end) -> LogAnalysisReport:
    return LogAnalysisReport(
        incident_id="INC-1",
        analyzed_from=start,
        analyzed_to=end,
        summary=summary,
        verification_status=VerificationStatus.PASSED,
    )


async def test_last_verified_report_is_published_as_html(tmp_path):
    # 여러 window의 리포트가 IncidentState.window_results에 순서대로 쌓이고,
    # 대표로는 마지막 window의 리포트를 쓴다 — ArtifactStore 없이도 같은 동작이다.
    reports = [
        _report("cpu spike in window A", _T0, _T1),
        _report("query timeout in window B", _T1, _T2),
    ]
    representative = reports[-1]
    rendered = to_incident_analysis_report(representative, Observations(), [])

    publication = await HtmlFileReportPublisher(output_dir=tmp_path).publish(rendered)

    files = list(tmp_path.glob("*.html"))
    assert len(files) == 1
    assert publication.text_length > 0
    content = files[0].read_text(encoding="utf-8")
    assert "query timeout in window B" in content
