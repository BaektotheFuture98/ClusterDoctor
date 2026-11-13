from datetime import datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.health_point import HealthPoint
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_orchestrator_agent.model.incident_report import IncidentAnalysisReport
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import IncidentClose, SessionContext
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.incident_scope_rows import (
    scope_rows,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.incident_timeline import (
    _health_signals,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.report_layout import (
    _overview,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import (
    render_text,
)

_T = datetime(2026, 11, 13, 10, 0, tzinfo=KST)
_END = _T + timedelta(minutes=20)


def test_rows_for_manual_analysis_are_empty():
    assert scope_rows(observed_start=_T, incident_end=None, close=IncidentClose.NOT_WAITED, session=None) == ()


def test_rows_show_close_session_and_stop():
    session = SessionContext(
        session_id="s1", sequence=3, last_allowed=True, previous_incident_id="i2",
        onset=_T - timedelta(minutes=40),
    )
    rows = dict(scope_rows(observed_start=_T, incident_end=_END, close=IncidentClose.SPAN_LIMIT, session=session))
    assert rows["마감 사유"] == "범위 상한"
    assert rows["세션"] == "s1 · 3번째 · 이전 incident i2"
    assert rows["장애 최초 관측"].startswith("2026-11-13 09:20:00")
    assert rows["추적"] == "장시간 미복구, 자동 추적 중단"


def test_range_row_has_no_microseconds():
    rows = dict(scope_rows(
        observed_start=_T, incident_end=_END + timedelta(microseconds=123456),
        close=IncidentClose.GREEN, session=None,
    ))
    assert "." not in rows["incident 범위"]


def test_html_and_text_show_rows():
    report = IncidentAnalysisReport(observations=Observations(), scope_rows=(("마감 사유", "green 확인"),))
    assert "<th>마감 사유</th><td>green 확인</td>" in _overview(report)
    assert "마감 사유: green 확인" in render_text(report)


def test_health_signals_use_incident_span():
    inside = HealthPoint(at=_T + timedelta(minutes=3), until=_T + timedelta(minutes=3), status="red")
    after = HealthPoint(at=_END + timedelta(minutes=5), until=_END + timedelta(minutes=5), status="red")
    observations = Observations(
        requested=((_T - timedelta(minutes=5), _T + timedelta(minutes=1)),),
        health=(inside, after),
        incident_span=(_T, _END),
    )
    starts = [signal.start for signal in _health_signals(observations, ())]
    assert starts == [inside.at]
