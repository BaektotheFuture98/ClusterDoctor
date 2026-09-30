import asyncio
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport
from cluster_doctor.incident_orchestrator_agent.model.incident import (
    Incident,
    IncidentStatus,
)
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentAnalysisResult,
    StartIncident,
)
from cluster_doctor.incident_orchestrator_agent.model.report_delivery import (
    ReportPublication,
)
from cluster_doctor.incident_orchestrator_agent.service.incident_lifecycle.analyze_incident import (
    AnalyzeIncident,
)


def test_timeout_waits_for_worker_and_preserves_its_report():
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    incident = Incident(
        incident_id="i", cluster="c", trigger_time=now, kafka_receive_time=now
    )
    report = LogAnalysisReport(
        incident_id="i",
        analyzed_from=now,
        analyzed_to=now + timedelta(minutes=1),
        summary="preserved",
    )
    result = IncidentAnalysisResult(
        status=IncidentStatus.COMPLETED,
        report=report,
        gaps=("source gap",),
        analysis_calls=2,
    )

    class Analyzer:
        def analyze(self, request):
            time.sleep(0.03)
            return result

    publisher = MagicMock()
    publisher.publish = AsyncMock(return_value=ReportPublication())
    service = AnalyzeIncident(
        incident_analyzer=Analyzer(),
        report_publisher=publisher,
        incident_timeout_seconds=0.005,
    )
    outcome = asyncio.run(service.handle(StartIncident(incident, now, now, 0)))
    assert outcome.status is IncidentStatus.FAILED
    assert outcome.analysis_failed
    assert outcome.report is report
    assert outcome.analysis_calls == 2
    assert outcome.gaps == ("source gap",)
    assert publisher.publish.call_args.kwargs["analysis_failed"]


def test_publication_deduplicates_verification_issues():
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    report = LogAnalysisReport(
        incident_id="i",
        analyzed_from=now,
        analyzed_to=now,
        verification_issues=("same issue",),
    )
    result = IncidentAnalysisResult(
        status=IncidentStatus.COMPLETED,
        report=report,
        failed=True,
        gaps=("리포트 검증 불일치: same issue",),
    )
    publisher = MagicMock()
    publisher.publish = AsyncMock(return_value=ReportPublication())
    service = AnalyzeIncident(incident_analyzer=MagicMock(), report_publisher=publisher)
    asyncio.run(service._deliver(result, result.failed, result.gaps))
    assert publisher.publish.call_args.kwargs["gaps"] == (
        "리포트 검증 불일치: same issue",
    )
