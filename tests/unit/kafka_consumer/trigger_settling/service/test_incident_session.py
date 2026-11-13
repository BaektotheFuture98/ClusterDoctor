from datetime import UTC, datetime, timedelta

from cluster_doctor.incident_orchestrator_agent.model.lifecycle import IncidentClose
from cluster_doctor.kafka_consumer.trigger_settling.service.incident_session import (
    INCIDENT_SPAN_LIMIT,
    MAX_INCIDENTS_PER_SESSION,
    Closing,
    continue_session,
    decide_close,
)

_S = datetime(2026, 11, 13, 1, 0, tzinfo=UTC)
_NOW = _S + timedelta(minutes=3)


def test_new_signals_keep_incident_open_even_if_green():
    assert decide_close(start=_S, now=_NOW, arrived=True, green=True) is None


def test_quiet_and_green_closes_now():
    assert decide_close(start=_S, now=_NOW, arrived=False, green=True) == Closing(_NOW, IncidentClose.GREEN)


def test_red_or_yellow_keeps_waiting():
    assert decide_close(start=_S, now=_NOW, arrived=False, green=False) is None


def test_health_failure_closes_now():
    assert decide_close(start=_S, now=_NOW, arrived=False, green=None) == Closing(_NOW, IncidentClose.HEALTH_UNKNOWN)


def test_span_limit_closes_at_limit_not_now():
    late = _S + INCIDENT_SPAN_LIMIT + timedelta(minutes=15)
    assert decide_close(start=_S, now=late, arrived=True, green=False) == Closing(
        _S + INCIDENT_SPAN_LIMIT, IncidentClose.SPAN_LIMIT
    )


def test_green_after_limit_ends_at_limit():
    late = _S + INCIDENT_SPAN_LIMIT + timedelta(minutes=1)
    assert decide_close(start=_S, now=late, arrived=False, green=True) == Closing(
        _S + INCIDENT_SPAN_LIMIT, IncidentClose.GREEN
    )


def test_session_continues_on_queue_or_not_green():
    assert continue_session(sequence=1, queued=True, green=None)
    assert continue_session(sequence=1, queued=False, green=False)
    assert not continue_session(sequence=1, queued=False, green=True)
    assert not continue_session(sequence=1, queued=False, green=None)


def test_session_stops_at_limit():
    assert not continue_session(sequence=MAX_INCIDENTS_PER_SESSION, queued=True, green=False)
