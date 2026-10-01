from datetime import UTC, datetime

from cluster_doctor.incident_analysis_agent.model.evidence import (
    Evidence,
    EvidenceSource,
)
from cluster_doctor.incident_analysis_agent.model.observations import (
    Observations,
    TimelineRow,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.incident_timeline import (
    project_timeline,
)

_UTC = UTC
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


def test_adjacent_events_keep_their_own_time_and_evidence():
    from datetime import timedelta

    evidence = tuple(
        Evidence(
            evidence_id=f"E-{i}",
            event_time=_MINUTE + timedelta(minutes=i),
            source=EvidenceSource.NODE_LOG,
            node_name="data-03",
            event_type=kind,
            severity="Warning",
            message=kind,
            raw=kind,
        )
        for i, kind in enumerate(("queue", "rejection", "gc"))
    )
    cards = project_timeline(Observations(), evidence)
    assert [c.start for c in cards] == [e.event_time for e in evidence]
    assert [c.evidence_refs for c in cards] == [(e.evidence_id,) for e in evidence]


def test_repeated_log_type_keeps_first_last_and_count():
    from datetime import timedelta

    evidence = tuple(
        Evidence(
            evidence_id=f"R-{i}",
            event_time=_MINUTE + timedelta(minutes=i),
            source=EvidenceSource.NODE_LOG,
            node_name="data-03",
            event_type="rejection",
            severity="Warning",
            message="rejected execution",
        )
        for i in range(3)
    )
    cards = project_timeline(Observations(), evidence)
    assert len(cards) == 1
    assert cards[0].start == evidence[0].event_time
    assert cards[0].end == evidence[-1].event_time
    assert "3건" in cards[0].representative_event


def test_lower_followup_is_observation_not_recovery():
    from datetime import timedelta

    observations = Observations(
        timeline=(
            TimelineRow(minute=_MINUTE, counts={"slowlog": 2}),
            TimelineRow(minute=_MINUTE + timedelta(minutes=1), counts={"slowlog": 0}),
        )
    )
    cards = project_timeline(observations)
    assert "회복" not in texts(cards)
    assert "slowlog" in texts(cards)


def texts(cards):
    return ' '.join(item.text for card in cards for item in (*card.impacts,*card.causes,*card.interpretations))


def test_counter_reset_is_not_recovery():
    from datetime import timedelta
    obs=Observations(timeline=(TimelineRow(minute=_MINUTE,counts={'node_metric':1},search_rejected_max=900),
        TimelineRow(minute=_MINUTE+timedelta(minutes=1),counts={'node_metric':1},search_rejected_max=0)))
    cards=project_timeline(obs)
    assert '회복' not in texts(cards)
    assert all(c.severity!='Critical' for c in cards)


def test_last_warning_is_not_ongoing_incident():
    e=Evidence(evidence_id='warn',event_time=_MINUTE,source=EvidenceSource.NODE_LOG,severity='Warning',message='last warning')
    cards=project_timeline(Observations(timeline=(TimelineRow(minute=_MINUTE),)),(e,))
    assert '지속' not in texts(cards)


def test_fallback_log_has_no_exact_timeline_time():
    e=Evidence(evidence_id='unknown',event_time=_MINUTE,source=EvidenceSource.NODE_LOG,
        severity='ERROR',time_origin='fallback',message='no timestamp')
    assert project_timeline(Observations(),(e,))==()


def test_parsed_log_keeps_exact_seconds():
    from datetime import timedelta
    at=_MINUTE+timedelta(seconds=7,milliseconds=123)
    e=Evidence(evidence_id='exact',event_time=at,source=EvidenceSource.NODE_LOG,severity='ERROR',message='exact error')
    assert project_timeline(Observations(),(e,))[0].start==at


def test_warn_level_remains_warning_in_timeline():
    e=Evidence(evidence_id='warn-level',event_time=_MINUTE,source=EvidenceSource.NODE_LOG,severity='WARN',message='warning')
    assert project_timeline(Observations(),(e,))[0].severity=='Warning'
