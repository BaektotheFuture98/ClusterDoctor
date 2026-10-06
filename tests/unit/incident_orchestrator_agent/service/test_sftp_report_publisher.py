import logging
from datetime import UTC, datetime
from pathlib import Path

from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport, VerificationStatus
from cluster_doctor.incident_orchestrator_agent.model.report_delivery import ReportPublication
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.output_mapping import (
    to_incident_analysis_report,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.publication.file.report_publisher import (
    ReportPublisher,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.publication.sftp.sftp_report_publisher import (
    SftpReportPublisher,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import (
    HtmlFileReportPublisher,
)

_T0 = datetime(2026, 10, 6, 9, 0, tzinfo=UTC)
_LOGGER = "cluster_doctor.incident_orchestrator_agent.service.report_delivery.publication.sftp.sftp_report_publisher"


def _report():
    report = LogAnalysisReport(
        incident_id="INC-1",
        analyzed_from=_T0,
        analyzed_to=_T0,
        summary="요약",
        verification_status=VerificationStatus.PASSED,
    )
    return to_incident_analysis_report(report, Observations(), [])


class StubInner(ReportPublisher):
    def __init__(self, publication):
        self.publication = publication
        self.calls = 0

    async def publish(self, report, *, gaps=(), analysis_failed=False):
        self.calls += 1
        return self.publication


class StubUploader:
    def __init__(self, result="/srv/reports/r.html", error=None):
        self.result = result
        self.error = error
        self.uploaded = []

    def upload(self, local_path):
        self.uploaded.append(Path(local_path))
        if self.error:
            raise self.error
        return self.result


async def test_html_publisher_returns_the_saved_path(tmp_path):
    publication = await HtmlFileReportPublisher(output_dir=tmp_path).publish(_report())

    assert publication.path is not None
    assert publication.path.parent == tmp_path
    assert publication.path.read_text(encoding="utf-8").startswith("<!doctype html>")
    assert publication.remote_path is None


async def test_saved_report_is_uploaded_and_the_remote_path_is_returned(tmp_path):
    saved = tmp_path / "r.html"
    saved.write_text("x", encoding="utf-8")
    uploader = StubUploader(result="/srv/reports/r.html")

    publication = await SftpReportPublisher(
        StubInner(ReportPublication(text_length=7, path=saved)), uploader
    ).publish(_report())

    assert uploader.uploaded == [saved]
    assert publication == ReportPublication(text_length=7, path=saved, remote_path="/srv/reports/r.html")


async def test_upload_failure_keeps_the_local_publication_and_does_not_raise(tmp_path, caplog):
    saved = tmp_path / "r.html"
    saved.write_text("x", encoding="utf-8")
    caplog.set_level(logging.ERROR, logger=_LOGGER)

    publication = await SftpReportPublisher(
        StubInner(ReportPublication(text_length=7, path=saved)),
        StubUploader(error=RuntimeError("SftpUploadError: connection refused")),
    ).publish(_report())

    assert publication == ReportPublication(text_length=7, path=saved)
    assert saved.exists()
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert "connection refused" in errors[0].getMessage()
    assert str(saved) in errors[0].getMessage()


async def test_nothing_is_uploaded_when_the_local_save_failed():
    uploader = StubUploader()
    inner = StubInner(ReportPublication(text_length=7, path=None))

    publication = await SftpReportPublisher(inner, uploader).publish(_report())

    assert uploader.uploaded == []
    assert publication.remote_path is None
    assert inner.calls == 1


async def test_gaps_and_analysis_failed_are_forwarded_to_the_inner_publisher(tmp_path):
    seen = {}

    class Recording(StubInner):
        async def publish(self, report, *, gaps=(), analysis_failed=False):
            seen["gaps"], seen["analysis_failed"] = gaps, analysis_failed
            return await super().publish(report, gaps=gaps, analysis_failed=analysis_failed)

    await SftpReportPublisher(
        Recording(ReportPublication(text_length=1)), StubUploader()
    ).publish(_report(), gaps=("수집 실패",), analysis_failed=True)

    assert seen == {"gaps": ("수집 실패",), "analysis_failed": True}
