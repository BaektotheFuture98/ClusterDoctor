from datetime import datetime
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import (
    MasterEvent,
    Observations,
)
from cluster_doctor.incident_analysis_agent.service.observation.correlation import build_cross_links
from cluster_doctor.incident_orchestrator_agent.model.incident_report import IncidentAnalysisReport
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.query_trend import QueryTrendPoint
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.query_trend import render_query_trend
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.report_layout import render_layout
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import render_text

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


def _event(at, line=FOLLOWER):
    return MasterEvent(timestamp=at, node="RC13-03", level="WARN", logger="o.e.t.TransportService", line=line)


def test_trend_extra_columns_dash_for_failed_points():
    obs = Observations(query_requests=(_query(_at(3, 18), "19.43"),))
    point = QueryTrendPoint(_at(3, 0), None, None, "failed", _at(3, 0), _at(4, 0))
    html = render_query_trend((point,), build_cross_links(obs))
    assert html.count("<td>—</td>") >= 4


def _report(**kw):
    obs = Observations(query_requests=(_query(_at(3, 18), "19.43"),), master_events=(_event(_at(3, 37)),))
    return IncidentAnalysisReport(observations=obs, narrative=None, verification_status="PASSED", cluster="es", **kw)


def test_text_report_has_verdict_and_node_lines():
    text = render_text(_report())
    assert "느린 요청은" in text or "판단할 수 없습니다" in text
    assert "노드 1대(RC6-18)가 마스터 요청에 늦게 응답했습니다" in text


def test_print_opens_folded_evidence():
    html = render_layout(_report(), _at(7, 0), gaps=(), analysis_failed=False, css="")
    assert "details::details-content{content-visibility:visible;display:block}" in html
    assert "beforeprint" in html
