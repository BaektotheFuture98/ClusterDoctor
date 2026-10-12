from datetime import datetime
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import Observations
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
    order = ['id="conclusion"', 'id="metrics"', 'id="query-trend"', 'id="causes"', 'id="actions"', '<details id="evidence">']
    positions = [html.index(marker) for marker in order]
    assert positions == sorted(positions)
    assert html.index('id="cross-source"') > html.index('<details id="evidence">')
    assert html.index('id="master-logs"') > html.index('<details id="evidence">')


def test_no_confidence_uncertainty_or_procedure_blocks():
    html = render_layout(_report(), datetime(2026, 10, 11, 19, 7, tzinfo=KST), gaps=(), analysis_failed=False, css="")
    for removed in ("확신도", "미확인 사항", "확인·조치", "LOW", "HIGH"):
        assert removed not in html
    assert "<li>조치 한 줄</li>" in html
