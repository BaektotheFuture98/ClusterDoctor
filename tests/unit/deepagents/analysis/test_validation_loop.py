from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from cluster_doctor.adapters.outbound.deepagents.analysis import subagent
from cluster_doctor.adapters.outbound.deepagents.analysis.session import _AnalysisSession
from cluster_doctor.adapters.outbound.deepagents.analysis.subagent import (
    AnalysisStatus,
    _run_validation_loop,
)
from cluster_doctor.domain.analysis.report import LogAnalysisReport, VerificationStatus
from cluster_doctor.domain.analysis.time_range import TimeRange
from cluster_doctor.domain.analysis.validation_types import (
    VerificationIssue,
    VerificationIssueType,
)

_T0 = datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc)
_T1 = datetime(2024, 1, 1, 13, 10, tzinfo=timezone.utc)


def _make_report(summary: str = "") -> LogAnalysisReport:
    return LogAnalysisReport(
        incident_id="INC-1", analyzed_from=_T0, analyzed_to=_T1, summary=summary
    )


def _make_session(report=None) -> _AnalysisSession:
    request = MagicMock()
    request.incident_id = "INC-1"
    session = _AnalysisSession(TimeRange(start=_T0, end=_T1), request)
    session.report = report
    session.report_ref = "RPT-INC-1-0" if report else None
    return session


def _make_seams(put_report_ref: str = "RPT-INC-1-1") -> MagicMock:
    seams = MagicMock()
    seams.store.list_evidence.return_value = []
    seams.store.get_evidence.return_value = []
    seams.store.get_observations.return_value.candidates = []
    seams.store.put_report.return_value = put_report_ref
    return seams


def _issue(issue_type: VerificationIssueType, reason: str = "x") -> VerificationIssue:
    return VerificationIssue(issue_type=issue_type, reason=reason)


def _patched(det_results):
    validate = patch.object(subagent, "validate_report", side_effect=det_results)
    grounding = patch.object(subagent, "GroundingValidator")
    return validate, grounding


def _det(*issues: str) -> MagicMock:
    return MagicMock(passed=not issues, issues=list(issues))


def test_validation_passed_returns_completed():
    session = _make_session(_make_report())
    seams = _make_seams()
    validate, grounding = _patched([_det()])
    with validate, grounding as grounding_cls:
        grounding_cls.return_value.validate.return_value = []
        result = _run_validation_loop(session=session, seams=seams)

    assert result.status == AnalysisStatus.COMPLETED
    assert result.report_ref == "RPT-INC-1-1"
    assert result.verification_status == VerificationStatus.PASSED
    saved = seams.store.put_report.call_args.args[1]
    assert saved.verification_status == VerificationStatus.PASSED


def test_no_report_returns_failed():
    session = _make_session(report=None)
    result = _run_validation_loop(session=session, seams=MagicMock())

    assert result.status == AnalysisStatus.FAILED
    assert result.report_ref is None
    assert result.verification_status == VerificationStatus.NOT_VERIFIED
    assert result.failure_reason


def test_candidate_ids_are_passed_to_deterministic_validation():
    session = _make_session(_make_report())
    seams = _make_seams()
    seams.store.get_observations.return_value.candidates = [MagicMock(candidate_id="C1")]
    validate, grounding = _patched([_det()])
    with validate as validate_mock, grounding as grounding_cls:
        grounding_cls.return_value.validate.return_value = []
        _run_validation_loop(session=session, seams=seams)

    assert validate_mock.call_args.kwargs["candidate_ids"] == {"C1"}


def test_report_mismatch_triggers_revise_and_revalidates():
    session = _make_session(_make_report())
    seams = _make_seams("RPT-INC-1-2")
    revised = _make_report("revised")
    seams.report_writer.revise_report.return_value = revised
    validate, grounding = _patched([_det("overclaim"), _det()])
    with validate, grounding as grounding_cls:
        grounding_cls.return_value.validate.side_effect = [
            [_issue(VerificationIssueType.REPORT_MISMATCH)],
            [],
        ]
        result = _run_validation_loop(session=session, seams=seams)

    seams.report_writer.revise_report.assert_called_once()
    assert result.status == AnalysisStatus.COMPLETED
    assert result.verification_status == VerificationStatus.PASSED
    assert seams.store.put_report.call_args.args[1].summary == "revised"


def test_revision_is_bounded_and_ends_in_mismatch():
    session = _make_session(_make_report())
    seams = _make_seams()
    seams.report_writer.revise_report.return_value = _make_report("still bad")
    rounds = subagent._MAX_VALIDATION_ROUNDS
    validate, grounding = _patched([_det("bad")] * (rounds + 1))
    with validate, grounding as grounding_cls:
        grounding_cls.return_value.validate.return_value = []
        result = _run_validation_loop(session=session, seams=seams)

    assert seams.report_writer.revise_report.call_count == rounds
    assert result.status == AnalysisStatus.COMPLETED
    assert result.verification_status == VerificationStatus.MISMATCH
    assert result.failure_reason == "bad"
    saved = seams.store.put_report.call_args.args[1]
    assert saved.verification_status == VerificationStatus.MISMATCH


def test_analysis_mismatch_reanalyzes_once_then_passes():
    session = _make_session(_make_report("first"))
    seams = _make_seams("RPT-INC-1-9")
    fresh = _make_session(_make_report("second"))
    validate, grounding = _patched([_det(), _det()])
    with validate, grounding as grounding_cls, patch.object(
        subagent, "_reanalyze_window", return_value=fresh
    ) as reanalyze:
        grounding_cls.return_value.validate.side_effect = [
            [_issue(VerificationIssueType.ANALYSIS_MISMATCH, "spike not in raw")],
            [],
        ]
        result = _run_validation_loop(session=session, seams=seams)

    reanalyze.assert_called_once()
    assert reanalyze.call_args.kwargs["focus"] == "spike not in raw"
    assert session.reanalysis_count == 1
    assert session.report.summary == "second"
    assert result.verification_status == VerificationStatus.PASSED


def test_analysis_mismatch_after_reanalysis_budget_is_returned_as_mismatch():
    session = _make_session(_make_report())
    session.reanalysis_count = subagent._MAX_REANALYSIS_ATTEMPTS
    seams = _make_seams()
    validate, grounding = _patched([_det()])
    with validate, grounding as grounding_cls, patch.object(
        subagent, "_reanalyze_window"
    ) as reanalyze:
        grounding_cls.return_value.validate.return_value = [
            _issue(VerificationIssueType.ANALYSIS_MISMATCH, "spike not in raw")
        ]
        result = _run_validation_loop(session=session, seams=seams)

    reanalyze.assert_not_called()
    seams.report_writer.revise_report.assert_not_called()
    assert result.verification_status == VerificationStatus.MISMATCH
    assert result.failure_reason == "spike not in raw"


def test_only_unverifiable_issues_leave_report_not_verified():
    session = _make_session(_make_report())
    seams = _make_seams()
    validate, grounding = _patched([_det()])
    with validate, grounding as grounding_cls:
        grounding_cls.return_value.validate.return_value = [
            _issue(VerificationIssueType.UNVERIFIABLE, "raw 없음")
        ]
        result = _run_validation_loop(session=session, seams=seams)

    seams.report_writer.revise_report.assert_not_called()
    assert result.status == AnalysisStatus.COMPLETED
    assert result.verification_status == VerificationStatus.NOT_VERIFIED
