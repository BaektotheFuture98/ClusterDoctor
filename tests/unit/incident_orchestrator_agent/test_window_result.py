from datetime import UTC, datetime

from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_orchestrator_agent.model.window_result import WindowResult

_T0 = datetime(2024, 1, 1, 13, 0, tzinfo=UTC)
_T1 = datetime(2024, 1, 1, 13, 10, tzinfo=UTC)


def test_window_result_preserves_its_window_and_report():
    window = TimeRange(start=_T0, end=_T1)
    report = LogAnalysisReport(incident_id="INC-004", analyzed_from=_T0, analyzed_to=_T1)
    result = WindowResult(window=window, report=report)
    assert result.window == window
    assert result.report is report
