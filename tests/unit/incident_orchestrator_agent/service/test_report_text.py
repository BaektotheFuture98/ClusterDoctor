from datetime import datetime, timezone
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.observations import (
    Observations,
    SlowCandidate,
    TimelineRow,
)
from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    IncidentAnalysisReport,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import (
    render_text,
)

_UTC = timezone.utc


def _query_candidate(candidate_id: str, company: str, user: str, run_time: str) -> SlowCandidate:
    return SlowCandidate(
        candidate_id=candidate_id,
        source="es_query_log",
        timestamp=datetime(2026, 9, 30, 15, 22, tzinfo=_UTC),
        run_time=Decimal(run_time),
        cmd="agg",
        company=company,
        user=user,
    )


def test_render_text_excludes_offender_grouping():
    obs = Observations(
        candidates=(_query_candidate("C1", "마크로밀엠브레인", "mqtai02@embrain.com", "28.66"),)
    )
    report = IncidentAnalysisReport(observations=obs, evidence=())
    text = render_text(report)
    assert "가해자 집계" not in text
    assert "느린 개별 실행 로그" in text


def test_timeline_card_lines_include_source_grouped_citations():
    minute = datetime(2026, 9, 30, 15, 22, tzinfo=_UTC)
    row = TimelineRow(minute=minute, counts={"slowlog": 1}, search_rejected_max=1)
    obs = Observations(timeline=(row,))
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
            event_type="node_metric_heap",
            message="heap 78%",
        ),
    )
    report = IncidentAnalysisReport(observations=obs, evidence=evidence)
    text = render_text(report)
    assert "slowlog took=12s" in text and "heap 78%" in text and "원문 없음" not in text
    assert "E-1" not in text and "E-2" not in text
