from datetime import datetime, timedelta
from types import SimpleNamespace

import cluster_doctor.incident_analysis_agent.service.report_generation.incident_synthesis as module
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisReport,
    ReportRecommendation,
    RootCause,
)
from cluster_doctor.incident_analysis_agent.model.validation import (
    VerificationIssueType,
)
from cluster_doctor.incident_analysis_agent.service.report_generation.previous_incident import (
    previous_incident_digest,
)
from cluster_doctor.incident_analysis_agent.service.report_generation.prompts import (
    build_incident_synthesis_messages,
)
from cluster_doctor.incident_analysis_agent.service.report_generation.report_writer import (
    ReportWriter,
)

_T = datetime(2026, 11, 13, 10, 0, tzinfo=KST)
_REPORT = LogAnalysisReport(
    incident_id="i1",
    analyzed_from=_T,
    analyzed_to=_T + timedelta(minutes=20),
    summary="노드 이탈로 샤드가 재배치됐다.",
    root_causes=(RootCause(statement="data-3 GC 정지"),),
    recommendations=(ReportRecommendation(text="heap 확인"),),
)


def test_digest_has_range_summary_causes_and_actions():
    digest = previous_incident_digest(_REPORT)
    assert "10:00~10:20" in digest
    assert "노드 이탈로 샤드가 재배치됐다." in digest
    assert "원인: data-3 GC 정지" in digest
    assert "조치: heap 확인" in digest


def test_window_prior_summary_includes_previous_incident():
    text = ReportWriter._prior_summary(None, _REPORT)
    assert text.startswith("이전 incident:")


def test_synthesis_marks_previous_incident_as_reference():
    messages = build_incident_synthesis_messages(
        cluster="c", analyzed_from=_T, analyzed_to=_T + timedelta(minutes=20),
        previous_windows_json="{}", analysis_context="{}", previous_incident="요약",
    )
    user = messages[1]["content"]
    assert "--- 이전 incident 보고서 요약 (참고용, 이번 사건의 관측이 아니다) ---" in user
    assert user.index("이전 incident") < user.index("누적 관측값")


def test_synthesis_without_previous_incident_has_no_section():
    messages = build_incident_synthesis_messages(
        cluster="c", analyzed_from=_T, analyzed_to=_T + timedelta(minutes=20),
        previous_windows_json="{}", analysis_context="{}",
    )
    assert "이전 incident" not in messages[1]["content"]


def test_previous_incident_reaches_only_the_synthesis_draft(monkeypatch):
    calls = {"revise": [], "grounding": []}

    class _Writer:
        def draft_incident(self, **kwargs):
            calls["draft"] = kwargs
            return _REPORT

        def revise_report(self, *args, **kwargs):
            calls["revise"].append((args, kwargs))
            return _REPORT

    class _Validator:
        def __init__(self, **kwargs):
            pass

        def validate_with_coverage(self, report, evidence, **kwargs):
            calls["grounding"].append((report, evidence, kwargs))
            issue = SimpleNamespace(issue_type=VerificationIssueType.REPORT_MISMATCH, reason="표현")
            return ([issue] if len(calls["grounding"]) == 1 else []), 1

        def has_claims(self, report):
            return True

    monkeypatch.setattr(module, "GroundingValidator", _Validator)
    seams = SimpleNamespace(
        wiki_investigator=None, report_writer=_Writer(), call_llm=None, metric_thresholds=None
    )
    module.synthesize_incident(
        seams=seams, incident=SimpleNamespace(incident_id="i1", cluster="c"),
        window_reports=[_REPORT], observations=Observations(), evidence=[],
        previous_incident_report=_REPORT,
    )
    assert calls["draft"]["previous_incident"] is _REPORT
    assert len(calls["revise"]) == 1
    _, revise_kwargs = calls["revise"][0]
    assert "previous_incident" not in revise_kwargs
    assert "previous_incident_report" not in revise_kwargs
    assert len(calls["grounding"]) == 2
    for _, _, kwargs in calls["grounding"]:
        assert "previous_incident" not in kwargs
