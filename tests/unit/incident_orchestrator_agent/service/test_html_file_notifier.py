from datetime import UTC, datetime
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.evidence import (
    Evidence,
    EvidenceSource,
)
from cluster_doctor.incident_analysis_agent.model.observations import (
    Observations,
    SlowCandidate,
    TimelineRow,
)
from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    IncidentAnalysisReport,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import (
    render_report,
)

_UTC = UTC


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
    report = IncidentAnalysisReport(
        observations=Observations(candidates=(candidate,)), evidence=()
    )
    html = render_report(report)
    assert "가해자 집계 (관측값)" in html


def test_render_report_includes_timeline_citation_details():
    minute = datetime(2026, 9, 30, 15, 22, tzinfo=_UTC)
    row = TimelineRow(minute=minute, counts={"slowlog": 1}, search_rejected_max=1)
    evidence = (
        Evidence(
            evidence_id="E-1",
            event_time=minute,
            source=EvidenceSource.SLOWLOG,
            message="slowlog took=12s",
        ),
        Evidence(
            evidence_id="E-2",
            event_time=minute,
            source=EvidenceSource.NODE_METRIC,
            event_type="node_metric_rejected",
            message="search rejected 1",
        ),
    )
    report = IncidentAnalysisReport(
        observations=Observations(timeline=(row,)), evidence=evidence
    )
    html = render_report(report)
    assert "근거 원문 (출처별)" in html
    assert '<details class="timeline-observations">' in html
