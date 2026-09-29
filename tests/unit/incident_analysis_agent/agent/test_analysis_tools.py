from datetime import datetime, timezone
from unittest.mock import MagicMock

from cluster_doctor.incident_analysis_agent.agent.contracts import LogAnalysisRequest
from cluster_doctor.incident_analysis_agent.agent.tools import _build_tools
from cluster_doctor.incident_analysis_agent.model.basemodel.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.basemodel.report import (
    LogAnalysisReport,
    VerificationStatus,
)
from cluster_doctor.incident_analysis_agent.model.basemodel.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.model.state.analysis_session import AnalysisSession
from cluster_doctor.incident_analysis_agent.service.report_generation.schema import DraftReport

_T0 = datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc)
_T1 = datetime(2024, 1, 1, 13, 10, tzinfo=timezone.utc)


def _request() -> LogAnalysisRequest:
    return LogAnalysisRequest(
        incident_id="INC-1",
        cluster="c1",
        analysis_window=TimeRange(start=_T0, end=_T1),
    )


def _session() -> AnalysisSession:
    counter = iter(range(1, 1000))
    return AnalysisSession(
        TimeRange(start=_T0, end=_T1),
        _request(),
        new_evidence_id=lambda: f"E-INC-1-{next(counter)}",
    )


def _evidence(evidence_id: str = "E-INC-1-1") -> Evidence:
    return Evidence(
        evidence_id=evidence_id,
        event_time=_T0,
        source=EvidenceSource.NODE_LOG,
        message="line",
    )


def _tools(seams, delegation) -> dict:
    return {tool.name: tool for tool in _build_tools(seams, delegation)}


def test_write_report_saves_not_verified_report():
    delegation = _session()
    delegation.collected = MagicMock()
    delegation.evidence = [_evidence()]

    draft = DraftReport(summary="s")
    seams = MagicMock()
    seams.report_writer.draft_report.return_value = draft

    result = _tools(seams, delegation)["write_report"].invoke({"focus": "why"})

    assert "error" not in result
    # 초안(DraftReport, pydantic)과 확정 리포트(LogAnalysisReport, domain)는
    # 서로 다른 타입으로 둘 다 채워진다 — 하나가 다른 하나를 대신하지 않는다.
    assert isinstance(delegation.draft, DraftReport)
    assert isinstance(delegation.report, LogAnalysisReport)
    assert not isinstance(delegation.report, DraftReport)
    # 검증은 여기서 돌지 않는다 — write_report는 미검증 상태로 저장만 한다.
    assert delegation.report.verification_status == VerificationStatus.NOT_VERIFIED
    assert result["verification_status"] == str(VerificationStatus.NOT_VERIFIED)


def test_write_report_without_collecting_first_returns_error():
    delegation = _session()
    seams = MagicMock()

    result = _tools(seams, delegation)["write_report"].invoke({"focus": ""})

    assert "error" in result
    seams.report_writer.draft_report.assert_not_called()


def test_write_report_without_evidence_returns_error():
    delegation = _session()
    delegation.collected = MagicMock()
    delegation.evidence = []
    seams = MagicMock()

    result = _tools(seams, delegation)["write_report"].invoke({"focus": ""})

    assert "error" in result


def test_write_report_stops_after_max_attempts_and_keeps_last_report():
    delegation = _session()
    delegation.collected = MagicMock()
    delegation.evidence = [_evidence()]
    delegation.report_attempts = 2  # tools._MAX_REPORT_ATTEMPTS
    delegation.report = LogAnalysisReport(
        incident_id="INC-1", analyzed_from=_T0, analyzed_to=_T1
    )
    seams = MagicMock()

    result = _tools(seams, delegation)["write_report"].invoke({"focus": ""})

    assert "error" in result
    seams.report_writer.draft_report.assert_not_called()
