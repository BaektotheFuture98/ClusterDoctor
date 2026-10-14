from dataclasses import replace
from datetime import datetime
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_orchestrator_agent.model.evidence_citation import EvidenceCitation
from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    CauseAssessment,
    IncidentAnalysisReport,
    Narrative,
    Recommendation,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.report_layout import render_layout


def _report() -> IncidentAnalysisReport:
    query = QueryLogEntry(
        reg_date=datetime(2026, 10, 11, 19, 3, 18, tzinfo=KST), host="h", run_time=Decimal("19.43"),
        success="Y", s_date=0, e_date=0, date_range=365, keyword=("메이크업",), target_host="192.168.1.30",
        index_name="a", conditions=(), cmd="agg", service="", env="", project="", company="", user="",
        search_count=0, etc="", cluster="",
    )
    return IncidentAnalysisReport(
        observations=Observations(query_requests=(query,)),
        narrative=Narrative(
            headline="요약 문단",
            causes=(CauseAssessment(statement="원인 판단", mechanism="판단 설명"),),
            recommendations=(Recommendation(text="조치 한 줄"),),
        ),
        verification_status="PASSED",
        cluster="es",
    )


def test_sections_are_ordered_conclusion_first_and_evidence_folded():
    html = render_layout(_report(), datetime(2026, 10, 11, 19, 7, tzinfo=KST), gaps=(), analysis_failed=False, css="")
    order = ['id="conclusion"', 'id="metrics"', 'id="query-trend"', 'id="query-ranking"', '<details id="evidence">']
    positions = [html.index(marker) for marker in order]
    assert positions == sorted(positions)
    assert 'id="causes"' not in html and 'id="actions"' not in html and "요약 문단" not in html
    assert html.index("원인 판단") < html.index("판단 설명") < html.index("조치 한 줄") < html.index('id="query-trend"')
    assert html.index('id="cross-source"') > html.index('<details id="evidence">')
    assert html.index('id="master-logs"') > html.index('<details id="evidence">')


def _evidence(evidence_id: str, message: str) -> Evidence:
    return Evidence(evidence_id=evidence_id, event_time=datetime(2026, 10, 11, 19, 3, tzinfo=KST),
                    source=EvidenceSource.MASTER_LOG, message=message)


def test_contradicting_citations_are_listed_after_support_with_label():
    support, contradict, both = _evidence("E1", "지지 원문"), _evidence("E2", "반대 원문"), _evidence("E3", "양쪽 원문")
    cite = lambda e: EvidenceCitation(e.evidence_id, e)
    report = replace(_report(), narrative=Narrative(
        causes=(CauseAssessment(statement="원인 판단", mechanism="설명",
                                supporting=(cite(support), cite(both)), contradicting=(cite(contradict), cite(both))),),
    ))
    html = render_layout(report, datetime(2026, 10, 11, 19, 7, tzinfo=KST), gaps=(), analysis_failed=False, css="")
    citations = html[html.index('id="citations"'):]
    marker = citations.index("<h3>반증</h3>")
    assert 0 < citations.index("지지 원문") < marker
    assert "반증 · " not in citations[:marker]
    assert "양쪽 원문" not in citations[:marker]
    assert citations.index("반증 · ") > marker
    assert citations.index("반대 원문") > marker and "반증 · " in citations[marker:citations.index("반대 원문")]
    assert citations.count("양쪽 원문") == 1 and citations.index("양쪽 원문") > marker


def test_no_confidence_uncertainty_or_procedure_blocks():
    html = render_layout(_report(), datetime(2026, 10, 11, 19, 7, tzinfo=KST), gaps=(), analysis_failed=False, css="")
    for removed in ("확신도", "미확인 사항", "확인·조치", "LOW", "HIGH"):
        assert removed not in html
    assert "<li>조치 한 줄</li>" in html
