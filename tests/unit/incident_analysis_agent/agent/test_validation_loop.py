from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from cluster_doctor.incident_analysis_agent.agent.state import (
    initial_analysis_agent_state,
)
from cluster_doctor.incident_analysis_agent.agent.subagent import (
    finalize_update,
    project_result,
)
from cluster_doctor.incident_analysis_agent.model.analysis_contract import (
    LogAnalysisRequest,
)
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisReport,
    VerificationStatus,
)
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.model.validation import (
    VerificationIssue,
    VerificationIssueType,
)


def state_with_report():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    request = LogAnalysisRequest(
        incident_id="i",
        cluster="c",
        analysis_window=TimeRange(start=start, end=start + timedelta(minutes=1)),
        analysis_goal="approved goal",
    )
    state = initial_analysis_agent_state(request)
    return {
        **state,
        "collected": True,
        "report": LogAnalysisReport(
            incident_id="i",
            analyzed_from=start,
            analyzed_to=request.analysis_window.end,
        ),
    }


def test_validation_revises_then_marks_final_report_passed():
    state = state_with_report()
    seams = MagicMock()
    seams.report_writer.revise_report.return_value = state["report"].model_copy(
        update={"summary": "revised"}
    )
    with (
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.validate_report",
            side_effect=[MagicMock(issues=["bad"]), MagicMock(issues=[])],
        ),
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.GroundingValidator"
        ) as grounding,
    ):
        grounding.return_value.validate.return_value = []
        update = finalize_update(seams, state)
    assert update["report"].verification_status is VerificationStatus.PASSED
    assert update["report"].summary == "revised"
    assert seams.report_writer.revise_report.call_count == 1


def test_revision_is_bounded_to_two_and_last_revision_is_validated():
    state = state_with_report()
    seams = MagicMock()
    seams.report_writer.revise_report.return_value = state["report"]
    with (
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.validate_report",
            return_value=MagicMock(issues=["bad"]),
        ) as validate,
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.GroundingValidator"
        ) as grounding,
    ):
        grounding.return_value.validate.return_value = []
        update = finalize_update(seams, state)
    assert validate.call_count == 3
    assert seams.report_writer.revise_report.call_count == 2
    assert update["report"].verification_status is VerificationStatus.MISMATCH


def test_missing_report_returns_failed_summary_and_gap_without_exception():
    state = {**state_with_report(), "report": None, "gaps": ("no logs",)}
    result = project_result(state)
    assert result.report is None
    assert result.status.value == "FAILED"
    assert "no logs" in result.analysis_summary


def test_failed_reanalysis_retains_report_and_gap():
    state = {**state_with_report(), "evidence_sequence": 7}
    seams = MagicMock()
    mismatch = VerificationIssue(
        issue_type=VerificationIssueType.ANALYSIS_MISMATCH, reason="raw mismatch"
    )
    with (
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.validate_report",
            return_value=MagicMock(issues=[]),
        ),
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.GroundingValidator"
        ) as grounding,
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.report_update",
            return_value={},
        ),
    ):
        grounding.return_value.validate.return_value = [mismatch]
        update = finalize_update(seams, state)
    assert update["report"].verification_status is VerificationStatus.MISMATCH
    assert update["evidence_sequence"] == 7
    assert update["reanalysis_count"] == 1
    assert update["report"].incident_id == state["report"].incident_id
    assert "원문 불일치 재분석에 실패했다." in update["gaps"]


def test_successful_reanalysis_redrafts_from_collected_evidence_with_mismatch_focus():
    state = {
        **state_with_report(),
        "evidence": ("collected evidence",),
        "time_basis": "collected clock",
        "report_attempts": 2,
    }
    seams = MagicMock()
    mismatch = VerificationIssue(
        issue_type=VerificationIssueType.ANALYSIS_MISMATCH, reason="raw mismatch"
    )
    fresh_report = state["report"].model_copy(update={"summary": "fresh"})
    with (
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.validate_report",
            return_value=MagicMock(issues=[]),
        ),
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.GroundingValidator"
        ) as grounding,
        patch(
            "cluster_doctor.incident_analysis_agent.agent.tools.EvidenceCollector"
        ) as collector,
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.report_update",
            return_value={"report": fresh_report, "report_attempts": 1},
        ) as write,
    ):
        grounding.return_value.validate.side_effect = [[mismatch], []]
        update = finalize_update(seams, state)
    collector.assert_not_called()
    assert write.call_count == 1
    redraft_state = write.call_args.args[1]
    assert redraft_state["evidence"] == ("collected evidence",)
    assert redraft_state["report_attempts"] == 0
    assert "raw mismatch" in redraft_state["request"].analysis_goal
    assert "raw mismatch" in write.call_args.args[2]
    assert update["report"].verification_status is VerificationStatus.PASSED
    assert update["report_attempts"] == 3
    result = project_result({**state, **update})
    assert result.observations.time_basis == "collected clock"
    assert result.report.summary == "fresh"


def test_validation_exception_after_reanalysis_preserves_redrafted_report():
    state = state_with_report()
    mismatch = VerificationIssue(
        issue_type=VerificationIssueType.ANALYSIS_MISMATCH, reason="bad"
    )
    fresh_report = state["report"].model_copy(update={"summary": "fresh"})
    with (
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.validate_report",
            return_value=MagicMock(issues=[]),
        ),
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.GroundingValidator"
        ) as grounding,
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.report_update",
            return_value={"report": fresh_report},
        ),
    ):
        grounding.return_value.validate.side_effect = [
            [mismatch],
            RuntimeError("validation offline"),
        ]
        update = finalize_update(MagicMock(), state)
    assert update["execution_failed"]
    result = project_result({**state, **update})
    assert result.status.value == "FAILED"
    assert result.report.summary == "fresh"
    assert result.verification_status is VerificationStatus.NOT_VERIFIED


def test_reanalysis_is_never_repeated_after_a_second_analysis_mismatch():
    state = state_with_report()
    mismatch = VerificationIssue(
        issue_type=VerificationIssueType.ANALYSIS_MISMATCH, reason="bad"
    )
    with (
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.validate_report",
            return_value=MagicMock(issues=[]),
        ),
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.GroundingValidator"
        ) as grounding,
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.report_update",
            return_value={"report": state["report"]},
        ) as write,
    ):
        grounding.return_value.validate.return_value = [mismatch]
        update = finalize_update(MagicMock(), state)
    assert write.call_count == 1
    assert update["report"].verification_status is VerificationStatus.MISMATCH


def test_unverifiable_is_not_passed_and_does_not_trigger_revision():
    state = state_with_report()
    seams = MagicMock()
    issue = VerificationIssue(
        issue_type=VerificationIssueType.UNVERIFIABLE, reason="no raw"
    )
    with (
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.validate_report",
            return_value=MagicMock(issues=[]),
        ),
        patch(
            "cluster_doctor.incident_analysis_agent.agent.subagent.GroundingValidator"
        ) as grounding,
    ):
        grounding.return_value.validate.return_value = [issue]
        update = finalize_update(seams, state)
    assert update["report"].verification_status is VerificationStatus.NOT_VERIFIED
    seams.report_writer.revise_report.assert_not_called()
