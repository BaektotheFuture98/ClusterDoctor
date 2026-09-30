from datetime import datetime, timezone

from cluster_doctor.incident_analysis_agent.model.basemodel.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.evidence_citation import (
    cite,
)

_UTC = timezone.utc


def test_cite_includes_id_time_source_and_message():
    evidence = Evidence(
        evidence_id="E-abc-1",
        event_time=datetime(2026, 9, 30, 15, 22, 54, tzinfo=_UTC),
        source=EvidenceSource.QUERY_LOG,
        message="query runtime=28.66s",
    )
    assert cite(evidence) == (
        "[E-abc-1] | 2026-09-30 15:22:54 | es_query_log | query runtime=28.66s"
    )


def test_cite_includes_node_and_severity_when_present():
    evidence = Evidence(
        evidence_id="E-abc-2",
        event_time=datetime(2026, 9, 30, 15, 22, 54, tzinfo=_UTC),
        source=EvidenceSource.NODE_METRIC,
        node_name="RC12-07",
        severity="Critical",
        message="jvm_heap 92%",
    )
    assert cite(evidence) == (
        "[E-abc-2] | 2026-09-30 15:22:54 | node_metric | Critical | "
        "node=RC12-07 | jvm_heap 92%"
    )
