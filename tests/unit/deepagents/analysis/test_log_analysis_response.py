import pytest

from cluster_doctor.adapters.outbound.deepagents.analysis.contracts import (
    AnalysisStatus,
    LogAnalysisResponse,
)
from cluster_doctor.domain.analysis.report import VerificationStatus


def test_completed_result():
    r = LogAnalysisResponse(
        status=AnalysisStatus.COMPLETED,
        report_ref="RPT-INC-001-1",
        verification_status=VerificationStatus.PASSED,
    )
    assert r.status == AnalysisStatus.COMPLETED
    assert r.report_ref == "RPT-INC-001-1"
    assert r.failure_reason is None


def test_failed_result():
    r = LogAnalysisResponse(
        status=AnalysisStatus.FAILED,
        report_ref=None,
        verification_status=VerificationStatus.NOT_VERIFIED,
        failure_reason="insufficient evidence",
    )
    assert r.report_ref is None


def test_frozen():
    r = LogAnalysisResponse(
        status=AnalysisStatus.COMPLETED,
        report_ref="RPT-001",
        verification_status=VerificationStatus.PASSED,
    )
    with pytest.raises(Exception):
        r.status = AnalysisStatus.FAILED  # type: ignore[misc]
