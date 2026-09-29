from cluster_doctor.domain.diagnosis.validation_types import MismatchKind, ValidationIssue


def test_mismatch_kind_values():
    assert MismatchKind.ANALYSIS_MISMATCH == "analysis_mismatch"
    assert MismatchKind.REPORT_MISMATCH == "report_mismatch"
    assert MismatchKind.UNVERIFIABLE == "unverifiable"


def test_validation_issue_fields():
    issue = ValidationIssue(
        kind=MismatchKind.ANALYSIS_MISMATCH,
        description="E1 contradicts finding F1",
        affected_window=None,
        affected_evidence_refs=["E-INC-1-1"],
    )
    assert issue.kind == MismatchKind.ANALYSIS_MISMATCH
    assert "E-INC-1-1" in issue.affected_evidence_refs
