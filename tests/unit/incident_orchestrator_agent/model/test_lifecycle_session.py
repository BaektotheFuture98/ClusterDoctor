from datetime import datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_orchestrator_agent.model.incident import Incident
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentClose,
    SessionContext,
    StartIncident,
    tracking_stopped,
)

_T = datetime(2026, 11, 13, 10, 0, tzinfo=KST)


def test_following_keeps_pseudonyms_and_onset_and_accumulates_windows():
    first = SessionContext(session_id="s1", sequence=1, last_allowed=False, onset=_T)
    window = TimeRange(_T, _T + timedelta(minutes=5))
    second = first.following(incident_id="inc1", report=None, windows=(window,), last_allowed=False)
    third = second.following(incident_id="inc2", report=None, windows=(window,), last_allowed=True)
    assert third.sequence == 3
    assert third.previous_incident_id == "inc2"
    assert third.previous_windows == (window, window)
    assert third.pseudonyms is first.pseudonyms
    assert third.onset == _T
    assert third.last_allowed


def test_request_onset_is_session_onset_for_follow_on_incident():
    incident = Incident(incident_id="i", cluster="c", trigger_time=_T, kafka_receive_time=_T)
    later = _T + timedelta(minutes=20)
    session = SessionContext(session_id="s1", sequence=2, last_allowed=False, onset=_T)
    assert StartIncident(incident, later, later, 0.0, session=session).to_request().onset == _T
    assert StartIncident(incident, later, later, 0.0).to_request().onset == later


def test_tracking_stops_only_on_last_allowed_without_green():
    last = SessionContext(session_id="s1", sequence=3, last_allowed=True)
    assert tracking_stopped(IncidentClose.SPAN_LIMIT, last)
    assert tracking_stopped(IncidentClose.HEALTH_UNKNOWN, last)
    assert not tracking_stopped(IncidentClose.GREEN, last)
    assert not tracking_stopped(IncidentClose.SPAN_LIMIT, None)


def test_to_request_carries_session_fields():
    incident = Incident(incident_id="i", cluster="c", trigger_time=_T, kafka_receive_time=_T)
    session = SessionContext(session_id="s1", sequence=1, last_allowed=False)
    command = StartIncident(
        incident, _T, _T, 0.0,
        incident_end=_T + timedelta(minutes=20),
        close=IncidentClose.SPAN_LIMIT,
        session=session,
        health_gaps=("g",),
    )
    request = command.to_request()
    assert request.session is session
    assert request.close is IncidentClose.SPAN_LIMIT
    assert request.incident_end == _T + timedelta(minutes=20)
