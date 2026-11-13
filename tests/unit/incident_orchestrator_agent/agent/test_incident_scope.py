from datetime import datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.health_point import HealthPoint
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_orchestrator_agent.agent.incident_scope import (
    first_windows,
    scope_lines,
    with_incident_health,
)
from cluster_doctor.incident_orchestrator_agent.model.incident import Incident
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentAnalysisRequest,
    IncidentClose,
    SessionContext,
)

_T = datetime(2026, 11, 13, 10, 0, tzinfo=KST)
_END = _T + timedelta(minutes=20)
_INCIDENT = Incident(incident_id="i2", cluster="c", trigger_time=_T, kafka_receive_time=_T)
_PREV = LogAnalysisReport(
    incident_id="i1", analyzed_from=_T - timedelta(minutes=20), analyzed_to=_T,
    summary="이전 결론 문장",
)


def _request(session=None, **fields):
    return IncidentAnalysisRequest(
        incident=_INCIDENT, observed_start=_T, observed_end=_T, settling_wait_seconds=0.0,
        incident_end=fields.pop("incident_end", _END),
        close=fields.pop("close", IncidentClose.SPAN_LIMIT),
        session=session, **fields,
    )


def test_scope_lines_without_session_show_range_and_close():
    lines = scope_lines(_request())
    assert lines == ["incident 범위: 2026-11-13T10:00:00 ~ 2026-11-13T10:20:00 (마감: 범위 상한)"]


def test_scope_lines_for_follow_on_incident_include_previous_report():
    window = TimeRange(_T - timedelta(minutes=10), _T)
    session = SessionContext(
        session_id="s1", sequence=2, last_allowed=False, previous_incident_id="i1",
        previous_report=_PREV, previous_windows=(window,),
    )
    text = "\n".join(scope_lines(_request(session)))
    assert "세션 s1의 2번째 incident" in text
    assert "이전 incident i1" in text
    assert "참고용, 이번 incident의 근거가 아니다" in text
    assert "이전 결론 문장" in text


def test_scope_lines_without_incident_end_are_empty():
    assert scope_lines(_request(incident_end=None, close=IncidentClose.NOT_WAITED)) == []


def test_first_windows_follow_on_starts_at_incident_start():
    session = SessionContext(session_id="s1", sequence=2, last_allowed=False)
    assert first_windows(_request(session)) == (TimeRange(_T, _T + timedelta(minutes=10)),)


def test_first_windows_first_incident_leads_onset():
    session = SessionContext(session_id="s1", sequence=1, last_allowed=False)
    assert first_windows(_request(session))[0].start == _T - timedelta(minutes=5)


def test_with_incident_health_prepends_history_and_sets_span():
    settled = HealthPoint(at=_T, until=_T, status="red")
    analysed = HealthPoint(at=_END + timedelta(minutes=5), until=_END + timedelta(minutes=5), status="green")
    merged = with_incident_health(Observations(health=(analysed,)), _request(health_history=(settled,)))
    assert merged.health == (settled, analysed)
    assert merged.incident_span == (_T, _END)


def test_with_incident_health_keeps_manual_observations():
    observations = Observations()
    assert with_incident_health(observations, _request(incident_end=None)) is observations
