from dataclasses import replace
from datetime import datetime
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import (
    MasterEvent,
    Observations,
    SourceWindowStatus,
)
from cluster_doctor.incident_analysis_agent.service.observation.correlation import build_cross_links
from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests
from cluster_doctor.incident_orchestrator_agent.model.incident_report import IncidentAnalysisReport
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.query_trend import QueryTrendPoint
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.conclusion import (
    render_conclusion,
    render_metric_cards,
    verdict_line,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.cross_source import render_cross_source
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


def test_empty_and_failed_query_log_are_not_reported_as_no_slow():
    empty = build_cross_links(Observations())
    assert verdict_line(empty).headline == "수집된 쿼리 실행 로그가 없습니다."
    failed = build_cross_links(Observations(source_statuses=(
        SourceWindowStatus("es_query_log", _at(0, 0), _at(1, 0), "failed", None, _at(2, 0), error="x"),)))
    assert verdict_line(failed).headline == "쿼리 실행 로그를 수집하지 못해 지연 범위를 판단할 수 없습니다."
    cards = render_metric_cards((), failed)
    assert "미확인" in cards.split("이상 실행")[1]


def test_minute_sentence_not_printed_when_slow_query_overlaps_a_wait():
    # 16초 쿼리가 19:02:55에 기록되고 대기 구간 19:02:54.6~19:03:08 과 겹친다.
    obs = Observations(query_requests=(_query(_at(2, 55), "16"),),
                       master_events=(_event(_at(3, 8)),))
    html = render_cross_source(build_cross_links(obs))
    assert "19:02 지연에는" not in html


def test_minute_sentence_still_printed_without_any_wait():
    obs = Observations(query_requests=(_query(_at(2, 55), "16"),))
    assert "19:02 지연에는 대응하는" in render_cross_source(build_cross_links(obs))


def test_overlap_table_hides_empty_rows_and_caps_at_twenty():
    events = [_event(_at(0, 20 + i), FOLLOWER.replace("RC6-18", f"N{i:02d}").replace("13.4s/13415ms", "1.0s/1000ms"))
              for i in range(5)]
    queries = tuple(_query(_at(3, 30, i * 1000), "10.88") for i in range(1))
    late = [_event(_at(3, 37 + i // 10, 0), FOLLOWER.replace("RC6-18", f"Q{i:02d}")) for i in range(25)]
    html = render_cross_source(build_cross_links(Observations(
        query_requests=queries, master_events=tuple(events + late))))
    assert "겹치는 느린 쿼리가 없는 마스터 대기 경고 5건" in html
    assert "N00 · " not in html
    assert "외 5건" in html


def test_node_log_wording_by_status():
    def render(status, rows, error=""):
        obs = Observations(master_events=(_event(_at(3, 37)),), source_statuses=(
            SourceWindowStatus("node_log", _at(3, 0), _at(4, 0), status, rows, _at(5, 0),
                               host="192.168.0.143", error=error),))
        return render_cross_source(build_cross_links(obs))
    assert "조회 성공 · 0줄" in render("ok", 0)
    assert "수집 실패 · boom" in render("failed", None, "boom")
    assert "7줄 수집" in render("ok", 7)
    assert "상태 미확인" in render("skipped", None)
    assert "조회 성공" not in render("skipped", 0)


def test_failed_master_log_uses_collection_note_when_no_overlaps():
    html = render_cross_source(build_cross_links(Observations()), master_note_text="수집 실패: ssh")
    assert "수집 실패: ssh" in html
    assert "연결할 마스터 대기 경고가 수집되지 않았습니다" not in html
    assert "연결할 마스터 대기 경고가 수집되지 않았습니다" in render_cross_source(build_cross_links(Observations()))


def test_trend_extra_columns_dash_for_failed_points():
    obs = Observations(query_requests=(_query(_at(3, 18), "19.43"),))
    point = QueryTrendPoint(_at(3, 0), None, None, "failed", _at(3, 0), _at(4, 0))
    html = render_query_trend((point,), build_cross_links(obs))
    assert html.count("<td>—</td>") >= 4


def test_conclusion_says_node_not_data_node():
    obs = Observations(master_events=(_event(_at(3, 37)),))
    html = render_conclusion(build_cross_links(obs))
    assert "데이터 노드" not in html and "노드 1대(RC6-18)" in html


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
