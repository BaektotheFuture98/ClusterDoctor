from datetime import datetime, timedelta, timezone

from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisReport,
    VerificationStatus,
)
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_orchestrator_agent.agent.adapter import (
    _DeepAgentIncidentAnalyzer,
)
from cluster_doctor.incident_orchestrator_agent.agent.state import (
    initial_main_agent_state,
)
from cluster_doctor.incident_orchestrator_agent.model.incident import IncidentStatus
from cluster_doctor.incident_orchestrator_agent.model.window_result import WindowResult
from cluster_doctor.incident_analysis_agent.model.analysis_contract import (
    AnalysisStatus,
    WindowAnalysisResult,
)
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisStatus
from cluster_doctor.incident_orchestrator_agent.agent.analysis_subagent import (
    apply_analysis_result,
)


def test_projection_preserves_previous_issues_and_latest_mismatch_failure():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    window = TimeRange(start=start, end=start + timedelta(minutes=1))
    state = initial_main_agent_state(
        incident_id="i",
        observed_end=window.end,
        pending_windows=(),
        total_wait_seconds=0,
    )
    old = LogAnalysisReport(
        incident_id="i",
        analyzed_from=start,
        analyzed_to=window.end,
        verification_status=VerificationStatus.MISMATCH,
        verification_issues=("old issue",),
    )
    last = old.model_copy(update={"verification_issues": ("new issue",)})
    state = {
        **state,
        "status": IncidentStatus.COMPLETED,
        "window_results": (
            WindowResult(window=window, report=old),
            WindowResult(window=window, report=last),
        ),
        "latest_verification_status": VerificationStatus.MISMATCH,
    }
    result = _DeepAgentIncidentAnalyzer._result_from(None, state)
    assert result.failed
    assert result.report is None  # Incident narrative is assembled after state projection.
    assert any("old issue" in gap for gap in result.gaps)
    assert any("new issue" in gap for gap in result.gaps)


def test_no_report_expansion_is_not_successful_in_supervisor_response():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    window = TimeRange(start=start, end=start + timedelta(minutes=1))
    result = WindowAnalysisResult(
        window=window,
        status=LogAnalysisStatus.NEED_MORE_CONTEXT,
        verification_status=VerificationStatus.NOT_VERIFIED,
        gaps=("no evidence",),
    )
    response = apply_analysis_result({}, result)["structured_response"]
    assert response.status is AnalysisStatus.FAILED
    assert not response.has_report
    assert response.failure_reason == "no evidence"
