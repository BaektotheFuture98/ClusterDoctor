import re
from datetime import UTC, datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.observations import (
    NodeMetricRow,
    Observations,
)
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisReport,
    RootCause,
    VerificationStatus,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.output_mapping import (
    to_incident_analysis_report,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import (
    render_report,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.summary_view import (
    DEMO_GAP,
)

T0 = datetime(2026, 10, 1, 5, 2, tzinfo=UTC)


def build(
    nodes=(),
    status=VerificationStatus.PASSED,
    issues=(),
    confidence="High",
    recommendations=(),
):
    report = LogAnalysisReport(
        incident_id="I",
        analyzed_from=T0,
        analyzed_to=T0 + timedelta(minutes=10),
        summary="검색 거절",
        verification_status=status,
        verification_issues=issues,
        root_causes=(RootCause(statement="검색 부하 집중", confidence=confidence),),
        recommendations=recommendations,
    )
    obs = Observations(
        nodes=nodes,
        requested=((T0, T0 + timedelta(minutes=10)),),
        time_basis="event_time",
    )
    return to_incident_analysis_report(report, obs, [], cluster="prod-es")


def summary_of(html):
    return html.split('id="summary"', 1)[1].split("</section>", 1)[0]


def test_header_shows_cluster_time_range_and_basis():
    html = render_report(build())
    assert "<h1>prod-es</h1>" in html
    assert "ClusterDoctor · Incident Report" in html
    assert "2026-10-01 14:02 ─ 14:12 KST" in html
    assert "Time basis: event_time" in html
    assert "DEMO DATA" not in html


def test_demo_gap_becomes_header_badge_not_warning():
    html = render_report(build(), gaps=(DEMO_GAP,))
    assert "DEMO DATA" in html
    assert "실제 장애 분석 결과가 아닌 디자인 미리보기 데이터입니다." in html
    assert 'class="alert-card"' not in html


def test_severity_and_confidence_are_separate_labelled_areas():
    html = render_report(
        build(nodes=(NodeMetricRow(node="d1", samples=3, search_rejected_max=4),))
    )
    s = summary_of(html)
    assert "Observed severity" in s and "Root cause confidence" in s
    assert "CRITICAL" in s and "HIGH" in s
    assert "●" not in s
    assert s.index("Observed severity") < s.index("Root cause confidence")
    severity = s.split("Observed severity", 1)[1].split("Root cause confidence", 1)[0]
    assert "HIGH" not in severity
    assert "근거 검증" not in s and "<ul>" not in s.split("주요 관측")[0]


def test_summary_has_no_recommendation_list():
    html = render_report(build(recommendations=("큐 확인", "쿼리 확인")))
    assert "우선 확인할 것" not in summary_of(html)
    assert "큐 확인" not in summary_of(html)


def test_mismatch_promotes_verification_warning_below_summary():
    html = render_report(
        build(status=VerificationStatus.MISMATCH, issues=("claim A", "claim B"))
    )
    assert 'class="alert-card"' in html
    assert html.index('id="summary"') < html.index('class="alert-card"')
    alert = html.split('class="alert-card"', 1)[1].split("</section>", 1)[0]
    assert "Evidence verification issue" in alert
    assert "2 issues found" in alert
    assert "<details" in alert and "claim A" in alert and "claim B" in alert
    assert "근거 검증" not in summary_of(html)


def test_not_verified_also_promotes_warning():
    html = render_report(build(status=VerificationStatus.NOT_VERIFIED))
    assert "Evidence verification issue" in html


def test_passed_without_notices_has_no_warning_area():
    html = render_report(build())
    assert 'class="alert-card"' not in html
    assert "Evidence verification issue" not in html


def test_passed_with_gap_still_shows_notice_card_without_verification_title():
    html = render_report(build(), gaps=("SSH 수집 실패",))
    alert = html.split('class="alert-card"', 1)[1].split("</section>", 1)[0]
    assert "SSH 수집 실패" in alert
    assert "Evidence verification issue" not in alert


def test_unobserved_nodes_render_no_key_observations_block():
    html = render_report(build(nodes=(NodeMetricRow(node="d1", samples=0),)))
    assert "주요 관측" not in summary_of(html)


def test_observed_zero_is_rendered_and_unobserved_node_is_ignored():
    html = render_report(
        build(
            nodes=(
                NodeMetricRow(node="d1", samples=2, search_queue_max=0),
                NodeMetricRow(node="ghost", samples=0, search_rejected_max=99),
            )
        )
    )
    s = summary_of(html)
    assert "주요 관측" in s
    assert re.search(r"Search rejected</dt><dd[^>]*>0<", s)
    assert re.search(r"Search queue max</dt><dd[^>]*>0<", s)
    block = s.split('class="key-observations"', 1)[1].split("</dl>", 1)[0]
    assert "99" not in block and "ghost" not in block


def test_key_observations_pick_impact_node_and_cap_at_five():
    nodes = (
        NodeMetricRow(
            node="d1",
            samples=2,
            jvm_heap_max=70,
            search_queue_max=5,
            search_rejected_max=3,
            write_rejected_max=1,
        ),
        NodeMetricRow(node="d2", samples=2, search_rejected_max=9),
    )
    s = summary_of(render_report(build(nodes=nodes)))
    assert re.search(r"영향 노드</dt><dd[^>]*>d2<", s)
    assert re.search(r"Search rejected</dt><dd[^>]*>12<", s)
    assert re.search(r"JVM heap max</dt><dd[^>]*>70%<", s)
    assert s.count("<dt>") <= 5


def test_nav_has_exactly_six_items():
    html = render_report(build(recommendations=("조치 하나",)))
    nav = html.split('aria-label="리포트 목차"', 1)[1].split("</nav>", 1)[0]
    assert re.findall(r">([^<]+)</a>", nav) == [
        "요약",
        "사건 흐름",
        "원인 판단",
        "의심 요청",
        "조치",
        "근거",
    ]
