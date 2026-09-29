from datetime import datetime, timezone

from cluster_doctor.adapters.outbound.persistence.in_memory_artifact_store import (
    InMemoryArtifactStore,
)
from cluster_doctor.adapters.outbound.reporting.html_file_notifier import (
    HtmlFileReportPublisher,
)
from cluster_doctor.application.output_mapping import to_diagnosis_report
from cluster_doctor.domain.diagnosis.report import LogAnalysisReport, VerificationStatus

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
    store = InMemoryArtifactStore()
    refs = [
        store.put_report("INC-1", _report("cpu spike in window A", _T0, _T1)),
        store.put_report("INC-1", _report("query timeout in window B", _T1, _T2)),
    ]

    reports = [store.get_report(ref) for ref in refs]
    representative = reports[-1]
    rendered = to_diagnosis_report(
        representative,
        store.get_observations("INC-1"),
        store.list_evidence("INC-1"),
    )

    publication = await HtmlFileReportPublisher(output_dir=tmp_path).publish(rendered)

    files = list(tmp_path.glob("*.html"))
    assert len(files) == 1
    assert publication.text_length > 0
    content = files[0].read_text(encoding="utf-8")
    assert "query timeout in window B" in content
