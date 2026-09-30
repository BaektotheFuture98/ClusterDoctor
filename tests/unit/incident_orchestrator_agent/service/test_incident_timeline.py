from datetime import datetime, timezone

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.observations import Observations, TimelineRow
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.incident_timeline import (
    project_timeline,
)

_UTC = timezone.utc
_MINUTE = datetime(2026, 9, 30, 15, 22, tzinfo=_UTC)


def test_project_timeline_groups_card_citations_by_source():
    row = TimelineRow(minute=_MINUTE, counts={"slowlog": 1}, search_rejected_max=1)
    observations = Observations(timeline=(row,))
    evidence = (
        Evidence(
            evidence_id="E-1",
            event_time=_MINUTE,
            source=EvidenceSource.SLOWLOG,
            message="slowlog took=12s",
        ),
        Evidence(
            evidence_id="E-2",
            event_time=_MINUTE,
            source=EvidenceSource.NODE_METRIC,
            event_type="node_metric_rejected",
            message="search rejected 1",
        ),
    )

    cards = project_timeline(observations, evidence)

    assert len(cards) == 1
    card = cards[0]
    assert set(card.evidence_refs) == {"E-1", "E-2"}
    citations = "\n".join(card.citations)
    assert "slowlog 1건" in citations
    assert "node_metric 1건" in citations
    assert "[E-1]" in citations
    assert "[E-2]" in citations


def test_project_timeline_citations_empty_when_card_has_no_evidence():
    row = TimelineRow(minute=_MINUTE, counts={}, failed=True)
    observations = Observations(timeline=(row,))
    cards = project_timeline(observations, evidence=())
    assert cards[0].citations == ()
