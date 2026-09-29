import pytest

from cluster_doctor.adapters.outbound.deepagents.diagnosis.subagent import (
    DiagnosisResult,
    DiagnosisStatus,
)
from cluster_doctor.domain.diagnosis.report import VerificationStatus


def test_completed_result():
    r = DiagnosisResult(
        status=DiagnosisStatus.COMPLETED,
        report_ref="RPT-INC-001-1",
        verification_status=VerificationStatus.PASSED,
    )
    assert r.status == DiagnosisStatus.COMPLETED
    assert r.report_ref == "RPT-INC-001-1"
    assert r.failure_reason is None


def test_failed_result():
    r = DiagnosisResult(
        status=DiagnosisStatus.FAILED,
        report_ref=None,
        verification_status=VerificationStatus.NOT_VERIFIED,
        failure_reason="insufficient evidence",
    )
    assert r.report_ref is None


def test_frozen():
    r = DiagnosisResult(
        status=DiagnosisStatus.COMPLETED,
        report_ref="RPT-001",
        verification_status=VerificationStatus.PASSED,
    )
    with pytest.raises(Exception):
        r.status = DiagnosisStatus.FAILED  # type: ignore[misc]
