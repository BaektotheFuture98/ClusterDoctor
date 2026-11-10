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
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.cross_links_text import verdict_line
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.query_trend import QueryTrendPoint
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.query_trend import render_query_trend

FOLLOWER = (
    "Received response for a request that has timed out, sent [13.4s/13415ms] ago, "
    "timed out [3.4s/3402ms] ago, action [internal:coordination/fault_detection/follower_check], "
    "node [{RC6-18}{Vt2xoFlXTDmYm-BHlsgvBg}{JULUWPt3SymLu6RSbm2GyQ}{RC6-18}{192.168.0.143}"
    "{192.168.0.143:9300}{d}{tier=warm-03}], id [1]"
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


def test_trend_table_gets_slow_and_master_columns():
    point = QueryTrendPoint(_at(3, 0), 461, Decimal("19.43"), "ok", _at(3, 0), _at(4, 0))
    html = render_query_trend((point,), build_cross_links(_OBS))
    assert "<th>≥10s</th><th>마스터 대기 경고</th>" in html
    assert "<td>2</td><td>1</td>" in html


