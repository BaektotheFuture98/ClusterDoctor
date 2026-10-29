from datetime import datetime
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import (
    MasterEvent,
    NodeMetricRow,
    Observations,
    SourceWindowStatus,
)
from cluster_doctor.incident_analysis_agent.service.observation.correlation import build_cross_links

FOLLOWER = (
    "Received response for a request that has timed out, sent [13.4s/13415ms] ago, "
    "timed out [3.4s/3402ms] ago, action [internal:coordination/fault_detection/follower_check], "
    "node [{RC6-18}{Vt2xoFlXTDmYm-BHlsgvBg}{JULUWPt3SymLu6RSbm2GyQ}{RC6-18}{192.168.0.143}"
    "{192.168.0.143:9300}{d}{8.12.2}{partition=2, tier=warm-03}], id [3721132349]"
)


def _at(minute: int, second: int, micro: int = 0) -> datetime:
    return datetime(2026, 10, 11, 19, minute, second, micro, tzinfo=KST)


def _query(at: datetime, seconds: str, *, cmd: str = "agg", date_range: int = 365,
           index: str = "lucy_main_202510,lucy_main_202511", target: str = "192.168.1.30") -> QueryLogEntry:
    return QueryLogEntry(
        reg_date=at, host="172.16.147.155", run_time=Decimal(seconds), success="Y",
        s_date=20251001, e_date=20260930, date_range=date_range, keyword=("메이크업",),
        target_host=target, index_name=index, conditions=(), cmd=cmd, service="web",
        env="", project="", company="c", user="u", search_count=0, etc="", cluster="es",
    )


def _observations(**overrides) -> Observations:
    base = dict(
        query_requests=(
            _query(_at(0, 7), "16.03"),
            _query(_at(3, 18), "19.43"),
            _query(_at(3, 21), "16.67"),
            _query(_at(3, 28), "10.88"),
            _query(_at(3, 30), "0.40", cmd="search", date_range=1, index="lucy_main_v1_20261011"),
        ),
        master_events=(MasterEvent(timestamp=_at(3, 37, 967471), node="RC13-03", level="WARN",
                                   logger="o.e.t.TransportService", line=FOLLOWER),),
        nodes=(NodeMetricRow(node="RC6-18", samples=3, cpu_max=12, jvm_heap_max=50, node_ip="192.168.0.143"),),
        source_statuses=(SourceWindowStatus("node_log", _at(3, 0), _at(4, 0), "ok", 0, _at(5, 0),
                                            host="192.168.0.143"),),
    )
    base.update(overrides)
    return Observations(**base)


def test_overlap_split_by_query_time_assumption():
    [overlap] = build_cross_links(_observations()).overlaps
    # 대기 구간 19:03:24.55 ~ 19:03:37.97
    assert [q.seconds for q in overlap.both] == [Decimal("10.88")]
    assert sorted(q.seconds for q in overlap.one_assumption) == [Decimal("16.67"), Decimal("19.43")]


def test_minute_links_show_slow_minute_without_master_wait():
    by_minute = {m.minute: m for m in build_cross_links(_observations()).minutes}
    assert (by_minute[_at(0, 0)].slow_queries, by_minute[_at(0, 0)].master_waits) == (1, 0)
    assert (by_minute[_at(3, 0)].slow_queries, by_minute[_at(3, 0)].master_waits) == (3, 1)


def test_node_card_joins_metrics_log_status_and_targets():
    [card] = build_cross_links(_observations()).nodes
    assert (card.node, card.tier, card.metrics.cpu_max) == ("RC6-18", "warm-03", 12)
    assert [(s.status, s.row_count) for s in card.node_log] == [("ok", 0)]
    assert card.queries_targeting == 0


def test_profile_groups_by_shape_not_keyword():
    profile = build_cross_links(_observations()).profile
    assert (profile[0].cmd, profile[0].date_range, profile[0].index_count) == ("agg", 365, 2)
    assert (profile[0].total, profile[0].slow, profile[0].max_seconds) == (4, 4, Decimal("19.43"))
    assert profile[1].slow == 0


def test_no_master_events_gives_empty_overlaps_and_cards():
    links = build_cross_links(_observations(master_events=()))
    assert links.overlaps == () and links.nodes == ()


def test_wait_actions_count_target_nodes_across_all_master_lines():
    from cluster_doctor.incident_analysis_agent.service.observation.correlation import cross_links_payload
    other = FOLLOWER.replace("RC6-18", "RC6-19").replace("Vt2xoFlXTDmYm-BHlsgvBg", "Wt2xoFlXTDmYm-BHlsgvBh")
    events = tuple(
        MasterEvent(timestamp=at, node="RC13-03", level="WARN", logger="o.e.t.TransportService", line=line)
        for at, line in ((_at(3, 37), FOLLOWER), (_at(3, 40), FOLLOWER), (_at(4, 2), other))
    )
    [row] = build_cross_links(_observations(master_events=events)).wait_actions
    assert row.action == "internal:coordination/fault_detection/follower_check"
    assert (row.target_nodes, row.waits, row.late_responses) == (2, 3, 3)
    assert (row.first, row.last) == (_at(3, 37), _at(4, 2))
    [payload] = cross_links_payload(build_cross_links(_observations(master_events=events)), {})["master_wait_actions"]
    assert payload["target_nodes"] == 2 and payload["waits"] == 3
