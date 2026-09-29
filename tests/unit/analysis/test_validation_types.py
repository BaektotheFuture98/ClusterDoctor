import pytest

from cluster_doctor.domain.analysis.validation_types import (
    VerificationIssue,
    VerificationIssueType,
)


def test_verification_issue_type_values():
    assert VerificationIssueType.ANALYSIS_MISMATCH == "analysis_mismatch"
    assert VerificationIssueType.REPORT_MISMATCH == "report_mismatch"
    assert VerificationIssueType.UNVERIFIABLE == "unverifiable"


def test_verification_issue_fields():
    issue = VerificationIssue(
        issue_type=VerificationIssueType.ANALYSIS_MISMATCH,
        reason="E1 contradicts finding F1",
        evidence_refs=("E-INC-1-1",),
    )
    assert issue.issue_type == VerificationIssueType.ANALYSIS_MISMATCH
    assert "E-INC-1-1" in issue.evidence_refs


def test_verification_issue_is_frozen():
    issue = VerificationIssue(
        issue_type=VerificationIssueType.UNVERIFIABLE, reason="raw 없음"
    )
    with pytest.raises(Exception):
        issue.reason = "changed"  # type: ignore[misc]
