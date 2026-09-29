# tests/integration/test_analyze_incident_baseline.py
from datetime import datetime, timezone

from cluster_doctor.incident_analysis_agent.model.basemodel.report import (
    LogAnalysisReport,
)
from cluster_doctor.incident_analysis_agent.model.basemodel.time_range import TimeRange
from cluster_doctor.incident_orchestrator_agent.model.basemodel.incident import IncidentStatus
from cluster_doctor.incident_orchestrator_agent.model.state.incident_state import (
    IncidentState,
    WindowResult,
)

_T0 = datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc)
_T1 = datetime(2024, 1, 1, 14, 0, tzinfo=timezone.utc)
# TimeRange는 최대 10분 구간만 허용한다 — window_results의 window 필드에는
# 이 짧은 구간을 쓰고, LogAnalysisReport.analyzed_from/to는 별도 제약이 없다.
_W0 = datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc)
_W1 = datetime(2024, 1, 1, 13, 10, tzinfo=timezone.utc)


def _report(incident_id: str, frm=_T0, to=_T1, **kw) -> LogAnalysisReport:
    return LogAnalysisReport(incident_id=incident_id, analyzed_from=frm, analyzed_to=to, **kw)


def test_incident_state_initial_status():
    s = IncidentState(incident_id="INC-003")
    assert s.status == IncidentStatus.OPEN
    assert s.window_results == []


def test_incident_state_accumulates_window_results():
    s = IncidentState(incident_id="INC-004")
    window = TimeRange(start=_W0, end=_W1)
    s.window_results.append(WindowResult(window=window, report=_report("INC-004", summary="w1")))
    s.window_results.append(WindowResult(window=window, report=_report("INC-004", summary="w2")))
    assert len(s.window_results) == 2
    assert s.window_results[-1].report.summary == "w2"


def test_incident_state_next_evidence_id_is_monotonic_per_incident():
    s = IncidentState(incident_id="INC-005")
    assert s.next_evidence_id() == "E-INC-005-1"
    assert s.next_evidence_id() == "E-INC-005-2"
