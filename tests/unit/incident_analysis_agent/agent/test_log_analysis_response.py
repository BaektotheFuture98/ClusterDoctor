import pytest

from cluster_doctor.incident_analysis_agent.agent.contracts import (
    AnalysisStatus,
    LogAnalysisResponse,
)
from cluster_doctor.incident_analysis_agent.model.basemodel.report import VerificationStatus


def test_completed_result():
    r = LogAnalysisResponse(
        status=AnalysisStatus.COMPLETED,
        has_report=True,
        verification_status=VerificationStatus.PASSED,
    )
    assert r.status == AnalysisStatus.COMPLETED
    assert r.has_report is True
    assert r.failure_reason is None


def test_failed_result():
    r = LogAnalysisResponse(
        status=AnalysisStatus.FAILED,
        has_report=False,
        verification_status=VerificationStatus.NOT_VERIFIED,
        failure_reason="insufficient evidence",
    )
    assert r.has_report is False


def test_frozen():
    r = LogAnalysisResponse(
        status=AnalysisStatus.COMPLETED,
        has_report=True,
        verification_status=VerificationStatus.PASSED,
    )
    with pytest.raises(Exception):
        r.status = AnalysisStatus.FAILED  # type: ignore[misc]
