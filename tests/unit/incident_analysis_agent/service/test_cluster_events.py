from datetime import datetime
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import MasterEvent, Observations, TimelineRow
from cluster_doctor.incident_analysis_agent.service.observation.correlation import build_cross_links, cross_links_payload
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.cross_links_text import cluster_event_line
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.conclusion import render_conclusion
from tests.unit.incident_analysis_agent.service.test_correlation import _at, _query

HEALTH = "Cluster health status changed from [RED] to [YELLOW] (reason: [shards started [[idx][1]]])"
STALE = "[lucy_main_v1_20261014][1] marking unavailable shards as stale"
LEFT = "node-left[{RC1}{abc} reason: disconnected]"


def _ev(minute: int | None, line: str) -> MasterEvent:
    return MasterEvent(timestamp=_at(minute, 5) if minute is not None else None, node="m", level="INFO",
                       logger="o.e.c.r.a.AllocationService", line=line)


def _links(events):
    # 느린 분: 0, 3 / 관측된 느리지 않은 분: 1(느린 요청 없음, 빠른 요청 있음)
    return build_cross_links(Observations(
        query_requests=(_query(_at(0, 7), "16.0"), _query(_at(3, 7), "12.0"), _query(_at(1, 7), "0.4", cmd="s")),
        timeline=(TimelineRow(minute=_at(1, 0)),), master_events=tuple(events)))


def test_classify_kinds_and_transitions():
    links = _links([_ev(0, HEALTH), _ev(1, HEALTH), _ev(3, STALE), _ev(None, LEFT), _ev(0, "other line")])
    kinds = {k.kind: k for k in links.cluster_events.kinds}
    assert kinds["health"].count == 2 and kinds["health"].transitions == (("RED", "YELLOW", 2),)
    assert kinds["shard"].count == 1 and kinds["node_left"].count == 1
    assert kinds["node_left"].first is None
    assert kinds["health"].first == _at(0, 5) and kinds["health"].last == _at(1, 5)


def test_minute_alignment_ignores_untimed_events():
    ev = _links([_ev(0, HEALTH), _ev(1, HEALTH), _ev(None, LEFT)]).cluster_events
    assert (ev.slow_minutes, ev.slow_minutes_with_events) == (2, 1)
    assert (ev.other_minutes, ev.other_minutes_with_events) == (1, 1)


def test_no_events_is_empty():
    links = _links([_ev(0, "other")])
    assert not links.cluster_events.kinds
    assert "cluster_events" not in cross_links_payload(links, {})
    assert cluster_event_line(links) == ""


def test_payload_and_line():
    links = _links([_ev(0, HEALTH), _ev(1, HEALTH), _ev(3, STALE)])
    payload = cross_links_payload(links, {})["cluster_events"]
    assert payload["kinds"][0]["transitions"] == [{"from": "RED", "to": "YELLOW", "count": 2}]
    assert payload["slow_minutes_with_events"] == 2 and payload["not_slow_minutes_with_events"] == 1
    assert cluster_event_line(links) == (
        "클러스터 이벤트: health 전환 2건(RED→YELLOW 2) · 샤드 실패 1건 — 느린 분 2개 중 2개, 느리지 않은 분 1개 중 1개에서 발생")


def test_conclusion_renders_event_line():
    html = render_conclusion(_links([_ev(0, HEALTH)]))
    assert '<p class="hint">클러스터 이벤트: health 전환 1건' in html
    assert "클러스터 이벤트" not in render_conclusion(_links([]))
