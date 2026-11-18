import json
import logging
from datetime import datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisReport, ReportRecommendation, RootCause, VerificationStatus,
)
from cluster_doctor.incident_analysis_agent.service.report_generation import window_digest as module
from cluster_doctor.incident_analysis_agent.service.report_generation.window_digest import window_digest

START = datetime(2026, 11, 17, 14, 0, tzinfo=KST)


def _report(status, minute=0, summary="구간 요약"):
    return LogAnalysisReport(
        incident_id="i", analyzed_from=START + timedelta(minutes=minute),
        analyzed_to=START + timedelta(minutes=minute + 5),
        summary=summary, summary_evidence_refs=("E-1",),
        root_causes=(RootCause(statement="원인 문장", supporting_evidence_refs=("E-1",)),),
        recommendations=(ReportRecommendation(text="조치 문장", evidence_refs=("E-2",)),),
        evidence_refs=("E-1", "E-2"), verification_status=status)


def test_interpretations_travel_with_their_status_and_refs():
    (entry,) = json.loads(window_digest([_report(VerificationStatus.PASSED)]))["windows"]

    assert entry == {
        "from": "2026-11-17T14:00:00+09:00", "to": "2026-11-17T14:05:00+09:00",
        "verification_status": "PASSED", "summary": "구간 요약", "summary_evidence_refs": ["E-1"],
        "root_causes": [{"statement": "원인 문장", "supporting_evidence_refs": ["E-1"]}],
        "recommendations": [{"text": "조치 문장", "evidence_refs": ["E-2"]}],
    }


def test_mismatched_windows_keep_only_their_evidence_ids():
    text = window_digest([_report(VerificationStatus.MISMATCH), _report(VerificationStatus.NOT_VERIFIED, 5)])
    mismatched, unverified = json.loads(text)["windows"]

    assert mismatched["evidence_refs"] == ["E-1", "E-2"]
    assert "원인 문장" not in json.dumps(mismatched, ensure_ascii=False)
    assert unverified["root_causes"][0]["statement"] == "원인 문장"


def test_later_windows_are_dropped_first_over_the_cap(monkeypatch, caplog):
    reports = [_report(VerificationStatus.PASSED, minute * 5, summary="가" * 300) for minute in range(4)]
    monkeypatch.setattr(module, "WINDOW_DIGEST_MAX_CHARS", 1200)

    with caplog.at_level(logging.WARNING):
        windows = json.loads(window_digest(reports))["windows"]

    assert 0 < len(windows) < 4
    assert windows[0]["from"] == "2026-11-17T14:00:00+09:00"
    assert "앞쪽" in caplog.text


def test_no_windows_give_an_empty_digest(monkeypatch):
    assert window_digest([]) == ""
    monkeypatch.setattr(module, "WINDOW_DIGEST_MAX_CHARS", 10)
    assert window_digest([_report(VerificationStatus.PASSED)]) == ""
