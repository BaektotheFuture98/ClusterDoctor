from datetime import datetime

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_orchestrator_agent.model.evidence_citation import EvidenceCitation
from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    CauseAssessment, Finding, IncidentAnalysisReport, Narrative, Recommendation,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.verification_view import (
    unverified_draft, verification_notice, verification_problems,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.report_layout import render_layout
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import render_text

NOW = datetime(2026, 10, 14, 15, 0, tzinfo=KST)


def _evidence(id_, text):
    return Evidence(evidence_id=id_, event_time=NOW, source=EvidenceSource.MASTER_LOG, message=text)


def _narrative():
    support, counter = _evidence("E1", "지지 원문"), _evidence("E2", "반증 원문")
    return Narrative(
        headline="초안 요약",
        causes=(CauseAssessment(statement="초안 원인", mechanism="초안 메커니즘",
                                supporting=(EvidenceCitation("E1", support),),
                                contradicting=(EvidenceCitation("E2", counter),)),),
        findings=(Finding("Info", "초안 관측", detail="초안 관측 설명"),),
        recommendations=(Recommendation(text="초안 조치"),))


def _report(status="MISMATCH", narrative=True, issues=("E9 인용 없음",)):
    return IncidentAnalysisReport(observations=Observations(), evidence=(_evidence("E1", "지지 원문"), _evidence("E2", "반증 원문")),
                                  narrative=_narrative() if narrative else None, verification_status=status,
                                  verification_issues=issues, cluster="es")


def _html(report, gaps=(), failed=False):
    return render_layout(report, NOW, gaps=gaps, analysis_failed=failed, css="")


def test_notice_per_status():
    assert verification_notice(_report("PASSED", issues=()), analysis_failed=False) == ""
    assert verification_notice(_report("MISMATCH"), analysis_failed=False).startswith("AI 분석이 근거 검증을 통과하지 못해")
    assert verification_notice(_report("NOT_VERIFIED"), analysis_failed=False).startswith("AI 분석의 근거 검증을 마치지 못해")
    assert verification_notice(_report("PASSED", issues=()), analysis_failed=True).startswith("구간 분석 중 실패가 있어")
    assert verification_notice(_report("NOT_VERIFIED", narrative=False, issues=()), analysis_failed=False) == "AI 분석 결과가 없어 관측값만 표시합니다."


def test_notice_points_to_section_only_when_rendered():
    assert "검증되지 않은 AI 분석" in verification_notice(_report("MISMATCH"), analysis_failed=False)
    assert "검증되지 않은 AI 분석" not in verification_notice(_report("NOT_VERIFIED", narrative=False, issues=()), analysis_failed=False)


def test_problems_merge_issues_and_gap_diagnostics():
    gaps = ("리포트 검증 불일치: E9 인용 없음", "리포트 검증이 실패했다: TimeoutError", "SSH 수집 실패")
    assert verification_problems(_report(), gaps) == ("E9 인용 없음", "TimeoutError")


def test_draft_only_when_not_displayed():
    assert unverified_draft(_report("MISMATCH"), analysis_failed=False) is not None
    assert unverified_draft(_report("PASSED", issues=()), analysis_failed=False) is None
    assert unverified_draft(_report("PASSED", issues=()), analysis_failed=True) is not None
    assert unverified_draft(_report("MISMATCH", narrative=False), analysis_failed=False) is None


def test_html_mismatch_shows_unverified_section_inside_evidence():
    html = _html(_report("MISMATCH"), gaps=("리포트 검증 불일치: E9 인용 없음",))
    conclusion = html[html.index('id="conclusion"'):html.index('id="metrics"')]
    assert "AI 분석이 근거 검증을 통과하지 못해" in conclusion and "초안 메커니즘" not in conclusion
    assert "AI 분석에 실패해 관측값만 표시합니다." not in html
    section = html[html.index('<section id="unverified">'):]
    assert html.index('<details id="evidence">') < html.index('<section id="unverified">')
    for text in ("검증되지 않은 AI 분석", "근거 검증을 통과하지 못한 초안입니다", "<li>E9 인용 없음</li>", "초안 요약", "초안 원인", "초안 메커니즘",
                 "지지 원문", "반증 · ", "반증 원문", "초안 관측", "<li>초안 조치</li>"):
        assert text in section
    assert section.count("E9 인용 없음") == 1


def test_html_absent_when_passed_and_empty_problems_note():
    assert 'id="unverified"' not in _html(_report("PASSED", issues=()))
    html = _html(_report("NOT_VERIFIED", issues=()))
    assert "기록된 검증 문제가 없습니다." in html and "초안 원인" in html


def test_html_escapes_draft():
    report = _report("MISMATCH", issues=("<b>x</b>",))
    html = _html(report)
    assert "<b>x</b>" not in html and "&lt;b&gt;x&lt;/b&gt;" in html


def test_text_mirrors_html():
    text = render_text(_report("MISMATCH"))
    assert "AI 분석이 근거 검증을 통과하지 못해" in text
    block = text[text.index("검증되지 않은 AI 분석\n"):]
    for line in ("검증 문제", "E9 인용 없음", "초안 요약", "초안 원인", "초안 메커니즘", "지지 원문", "반증: ", "반증 원문", "초안 관측", "확인·조치: 초안 조치"):
        assert line in block
    passed = render_text(_report("PASSED", issues=()))
    assert "검증되지 않은 AI 분석\n" not in passed
