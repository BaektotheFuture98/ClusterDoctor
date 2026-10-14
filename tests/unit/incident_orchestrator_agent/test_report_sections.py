from dataclasses import replace
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
from cluster_doctor.incident_analysis_agent.service.observation.correlation import Verdict, build_cross_links
from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.query_trend import QueryTrendPoint
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.conclusion import (
    node_line,
    render_conclusion,
    render_metric_cards,
    verdict_line,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.cross_source import render_cross_source
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.query_trend import render_query_trend

FOLLOWER = (
    "Received response for a request that has timed out, sent [13.4s/13415ms] ago, "
    "timed out [3.4s/3402ms] ago, action [internal:coordination/fault_detection/follower_check], "
    "node [{RC6-18}{Vt2xoFlXTDmYm-BHlsgvBg}{JULUWPt3SymLu6RSbm2GyQ}{RC6-18}{192.168.0.143}"
    "{192.168.0.143:9300}{d}{tier=warm-03}], id [1]"
)
NODE_LEFT = (
    "node-left: node [{RC9-01}{abc}{def}{RC9-01}{192.168.0.9}{192.168.0.9:9300}{d}] reason: disconnected"
)


def _at(minute, second, micro=0):
    return datetime(2026, 10, 11, 19, minute, second, micro, tzinfo=KST)


def _query(at, seconds):
    return QueryLogEntry(
        reg_date=at, host="h", run_time=Decimal(seconds), success="Y", s_date=0, e_date=0,
        date_range=365, keyword=(), target_host="192.168.1.30", index_name="a,b", conditions=(),
        cmd="agg", service="", env="", project="", company="", user="", search_count=0, etc="", cluster="",
    )


_OBS = Observations(
    query_requests=(_query(_at(0, 7), "16.03"), _query(_at(3, 18), "19.43"), _query(_at(3, 28), "10.88")),
    master_events=(MasterEvent(timestamp=_at(3, 37, 967471), node="RC13-03", level="WARN",
                               logger="o.e.t.TransportService", line=FOLLOWER),),
    nodes=(NodeMetricRow(node="RC6-18", samples=1, cpu_max=12, jvm_heap_max=50, node_ip="192.168.0.143"),),
    source_statuses=(SourceWindowStatus("node_log", _at(3, 0), _at(4, 0), "ok", 0, _at(5, 0), host="192.168.0.143"),),
)
_OBS_WITH_LEFT = replace(_OBS, master_events=_OBS.master_events + (
    MasterEvent(timestamp=_at(3, 40), node="RC13-03", level="INFO", logger="o.e.c.s.MasterService", line=NODE_LEFT),))


def test_verdict_lines():
    links = build_cross_links(_OBS)
    shapes = (("agg", 365, 12),)
    specific = replace(links, verdict=Verdict("specific", shapes, Decimal("0.21"), Decimal("0.24"), 4, (), (5,)))
    wide = replace(links, verdict=Verdict("cluster_wide", shapes, Decimal("0.21"), Decimal("1.30"), 4, shapes, (5,)))
    assert "일부 쿼리 형태에서만" in verdict_line(specific).headline
    assert verdict_line(specific).detail == "주요 형태: agg·365일·인덱스 12개 5건"
    assert "여러 쿼리 형태" in verdict_line(wide).headline
    assert verdict_line(wide).detail == "함께 느려진 형태: agg·365일·인덱스 12개"
    assert "0.21" not in verdict_line(specific).detail and "비교한" not in verdict_line(wide).detail
    assert "판단할 수 없습니다" in verdict_line(links).headline


def test_conclusion_has_no_severity_banner_and_mentions_nodes():
    html = render_conclusion(build_cross_links(_OBS), causes_html="<h3>원인</h3>")
    assert 'id="conclusion"' in html and "<h3>원인</h3>" in html
    assert "노드 1대(RC6-18)" in html
    assert "LOW" not in html and "HIGH" not in html


def test_metric_cards():
    html = render_metric_cards(rank_query_requests(_OBS.query_requests), build_cross_links(_OBS))
    assert "3건" in html and "19.43s" in html and "1대" in html


def test_cross_source_section():
    html = render_cross_source(build_cross_links(_OBS))
    assert "두 가정 모두 겹침" in html and "한 가정에서만 겹침" in html
    assert "19:03:24.552" in html and "warm-03" in html and "조회 성공 · 0줄" in html
    assert "19:00 지연에는 대응하는 마스터 대기 경고가 수집되지 않았습니다" in html


def test_trend_table_gets_slow_and_master_columns():
    point = QueryTrendPoint(_at(3, 0), 461, Decimal("19.43"), "ok", _at(3, 0), _at(4, 0))
    html = render_query_trend((point,), build_cross_links(_OBS))
    assert "<th>≥10s</th><th>마스터 대기 경고</th>" in html
    assert "<td>2</td><td>1</td>" in html


def test_node_without_late_response_is_not_counted_as_late():
    links = build_cross_links(_OBS_WITH_LEFT)
    assert {c.node for c in links.nodes} == {"RC6-18", "RC9-01"}
    assert "RC9-01" not in node_line(links) and "1대(RC6-18)" in node_line(links)
    cards = render_metric_cards(rank_query_requests(_OBS.query_requests), links)
    assert "<strong>1대</strong>" in cards
    assert "RC9-01" in render_cross_source(links)
