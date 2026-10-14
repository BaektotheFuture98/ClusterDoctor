from dataclasses import replace
from datetime import datetime
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.service.observation.correlation import CrossLinks, Verdict
from cluster_doctor.incident_orchestrator_agent.model.evidence_citation import EvidenceCitation
from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    CauseAssessment, Finding, IncidentAnalysisReport, Narrative, Recommendation,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.cross_links_text import verdict_line
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.conclusion import render_conclusion
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.report_layout import render_layout
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import render_text

NOW = datetime(2026, 10, 14, 15, 0, tzinfo=KST)
A, B, C, D = ("agg", 365, 12), ("agg", 181, 6), ("count", 365, 12), ("bulk", 1, 1)


def _links(verdict):
    return CrossLinks(slow_threshold=Decimal(10), overlaps=(), minutes=(), nodes=(), profile=(), verdict=verdict)


def _query():
    return QueryLogEntry(
        reg_date=datetime(2026, 10, 14, 14, 3, tzinfo=KST), host="h", run_time=Decimal("19.43"), success="Y",
        s_date=0, e_date=0, date_range=365, keyword=("k",), target_host="192.168.1.30", index_name="a", conditions=(),
        cmd="agg", service="", env="", project="", company="", user="", search_count=0, etc="", cluster="")


def _report(narrative, status="PASSED", evidence=()):
    return IncidentAnalysisReport(observations=Observations(query_requests=(_query(),)), evidence=evidence,
                                  narrative=narrative, verification_status=status, cluster="es")


def _narrative(**kw):
    return Narrative(
        headline="숨겨질 요약",
        causes=(CauseAssessment(statement="원인 문장", mechanism="메커니즘 문단"),),
        recommendations=(Recommendation(text="조치 하나"), Recommendation(text="조치 둘")),
        findings=(Finding("Info", "관측 제목", detail="관측 설명"),), **kw)


def _html(report, **kw):
    return render_layout(report, NOW, gaps=(), analysis_failed=kw.get("analysis_failed", False), css="")


def test_section_order_and_removed_sections():
    html = _html(_report(_narrative()))
    order = ['id="conclusion"', "원인 문장", "메커니즘 문단", "조치 하나", "조치 둘", 'id="metrics"', 'id="query-trend"',
             'id="query-ranking"', '<details id="evidence">']
    positions = [html.index(marker) for marker in order]
    assert positions == sorted(positions)
    assert 'id="causes"' not in html and 'id="actions"' not in html
    assert html.count("<li>조치 하나</li>") == 1


def test_summary_is_not_shown_in_html_or_text():
    report = _report(_narrative())
    assert "숨겨질 요약" not in _html(report)
    assert "숨겨질 요약" not in render_text(report)
    assert 'class="report-headline"' not in _html(report)


def test_headline_citations_are_not_listed():
    ev = Evidence(evidence_id="E1", event_time=datetime(2026, 10, 14, 14, 0, tzinfo=KST),
                  source=EvidenceSource.MASTER_LOG, message="요약에만 인용된 원문")
    narrative = _narrative(headline_citations=(EvidenceCitation("E1", ev),))
    report = _report(narrative, evidence=(ev,))
    assert 'id="citations"' not in _html(report)
    assert "요약에만 인용된 원문" not in render_text(report).split("결론")[1].split("핵심 요약")[0]


def test_findings_are_first_inside_folded_evidence():
    html = _html(_report(_narrative()))
    evidence = html[html.index('<details id="evidence">'):]
    assert evidence.index('id="findings"') < evidence.index('id="cross-source"')
    assert evidence.index("관측 제목") < evidence.index("관측 설명")
    assert "관측 제목" not in html[:html.index('<details id="evidence">')]
    text = render_text(_report(_narrative()))
    assert text.index("원인 문장") < text.index("주요 관측") < text.index("관측 제목")
    assert text.count("조치 하나") == 1


def test_specific_verdict_with_single_widened_shape():
    verdict = Verdict("specific", (A, B, C, D, ("x", 1, 1), ("y", 1, 1), ("z", 1, 1), ("w", 1, 1)),
                      Decimal("0.2"), Decimal("0.3"), 4, (B,), (40, 21, 12, 3, 2, 2, 1, 1))
    line = verdict_line(_links(verdict))
    assert line.headline == "느린 요청은 일부 쿼리 형태에서만 나왔고, 같은 시각 다른 요청은 느려지지 않았습니다."
    assert line.detail == ("주요 형태: agg·365일·인덱스 12개 40건, agg·181일·인덱스 6개 21건, count·365일·인덱스 12개 12건 외 5개 형태"
                           " · agg·181일·인덱스 6개는 같은 시각 10초 미만 요청도 함께 느려졌습니다")


def test_widened_note_uses_actual_threshold():
    links = replace(_links(Verdict("specific", (A,), None, None, 2, (A,), (3,))), slow_threshold=Decimal(5))
    assert "같은 시각 5초 미만" in verdict_line(links).detail


def test_cluster_wide_lists_widened_shapes():
    verdict = Verdict("cluster_wide", (A, B, C), Decimal("0.2"), Decimal("1.3"), 3, (A, B), (5, 4, 3))
    line = verdict_line(_links(verdict))
    assert line.headline.startswith("느린 시각에는 여러 쿼리 형태의 요청이 함께 느려졌습니다.")
    assert line.detail == "함께 느려진 형태: agg·365일·인덱스 12개, agg·181일·인덱스 6개"
    assert "0.2" not in line.detail and "비교" not in line.detail


def test_undetermined_and_empty_details():
    undetermined = verdict_line(_links(Verdict("undetermined", (A,), None, None, slow_counts=(7,))))
    assert "판단할 수 없습니다" in undetermined.headline and undetermined.detail == "주요 형태: agg·365일·인덱스 12개 7건"
    assert verdict_line(_links(Verdict("no_slow", (), None, None))).detail == ""
    assert verdict_line(_links(Verdict("no_query_log", (), None, None))).detail == ""


def test_conclusion_renders_headline_bold_and_detail_hint_escaped():
    verdict = Verdict("specific", (("a<b", 1, 1),), None, None, slow_counts=(2,))
    html = render_conclusion(_links(verdict))
    assert '<p class="verdict">느린 요청은 일부' in html
    assert '<p class="hint">주요 형태: a&lt;b·1일·인덱스 1개 2건</p>' in html


def test_fallback_narrative_shows_verdict_and_unconfirmed_cause():
    report = _report(_narrative(), status="NOT_VERIFIED")
    html = _html(report)
    conclusion = html[html.index('id="conclusion"'):html.index('id="metrics"')]
    assert 'class="verdict"' in conclusion
    assert "<h3>수집된 자료로 지연 원인을 확정할 수 없습니다.</h3>" in conclusion
    assert "관측된 실행 로그에서" not in html and "메커니즘 문단" not in html
    failed = _html(_report(_narrative()), analysis_failed=True)
    assert "AI 분석에 실패해 관측값만 표시합니다." in failed
    assert "수집된 자료로 지연 원인을 확정할 수 없습니다." in render_text(report)
