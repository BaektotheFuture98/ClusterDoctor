from cluster_doctor.incident_analysis_agent.model.basemodel.observations import Observations, SlowCandidate
from cluster_doctor.incident_orchestrator_agent.model.basemodel.incident_analysis_report import (
    IncidentAnalysisReport,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import (
    render_report,
    _sections_from_report,
)

from datetime import datetime, timezone
from decimal import Decimal

_UTC = timezone.utc


def test_render_report_includes_offender_section():
    candidate = SlowCandidate(
        candidate_id="C1",
        source="es_query_log",
        timestamp=datetime(2026, 9, 30, 15, 22, tzinfo=_UTC),
        run_time=Decimal("28.66"),
        cmd="agg",
        company="마크로밀엠브레인",
        user="mqtai02@embrain.com",
    )
    report = IncidentAnalysisReport(observations=Observations(candidates=(candidate,)), evidence=())
    html = render_report(report)
    assert "가해자 집계 (관측값)" in html

    sections = _sections_from_report(report)
    assert any(section.title == "가해자 집계 (관측값)" for section in sections)
