import asyncio
from datetime import datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.health_point import HealthPoint
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_orchestrator_agent.model.incident import (
    Incident,
    IncidentStatus,
)
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentAnalysisResult,
    IncidentClose,
    SessionContext,
    StartIncident,
)
from cluster_doctor.incident_orchestrator_agent.model.report_delivery import (
    ReportPublication,
)
from cluster_doctor.incident_orchestrator_agent.service.incident_lifecycle.analyze_incident import (
    AnalyzeIncident,
)

_T = datetime(2026, 11, 13, 10, 0, tzinfo=KST)


class _Analyzer:
    def __init__(self):
        self.requests = []

    def analyze(self, request):
        self.requests.append(request)
        return IncidentAnalysisResult(status=IncidentStatus.COMPLETED)


class _Publisher:
    def __init__(self):
        self.calls = []

    async def publish(self, report, *, gaps, analysis_failed):
        self.calls.append((report, gaps, analysis_failed))
        return ReportPublication()


def _service(timeout=60.0):
    analyzer, publisher = _Analyzer(), _Publisher()
    return AnalyzeIncident(
        incident_analyzer=analyzer, report_publisher=publisher, incident_timeout_seconds=timeout
    ), analyzer, publisher


def _command(**fields):
    incident = Incident(incident_id="i", cluster="c", trigger_time=_T, kafka_receive_time=_T)
    settling = fields.pop("settling", 0.0)
    return StartIncident(incident, _T, _T, settling, **fields)


def test_settling_wait_does_not_shrink_analysis_time():
    service, analyzer, _ = _service(timeout=60.0)
    outcome = asyncio.run(service.handle(_command(settling=120.0)))
    assert len(analyzer.requests) == 1
    assert outcome.status is IncidentStatus.COMPLETED


def test_request_carries_session_and_health():
    service, analyzer, _ = _service()
    session = SessionContext(session_id="s1", sequence=2, last_allowed=False)
    point = HealthPoint(at=_T, until=_T, status="red")
    asyncio.run(service.handle(_command(
        close=IncidentClose.SPAN_LIMIT,
        incident_end=_T + timedelta(minutes=20),
        health_history=(point,),
        session=session,
    )))
    request = analyzer.requests[0]
    assert request.session is session
    assert request.health_history == (point,)
    assert request.close is IncidentClose.SPAN_LIMIT


def test_health_gaps_are_published():
    service, _, publisher = _service()
    asyncio.run(service.handle(_command(health_gaps=("health 확인 불가로 incident를 마감했다",))))
    assert "health 확인 불가로 incident를 마감했다" in publisher.calls[0][1]
