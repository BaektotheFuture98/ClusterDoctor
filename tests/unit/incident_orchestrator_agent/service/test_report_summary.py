from dataclasses import replace
import pytest
from cluster_doctor.incident_analysis_agent.model.observations import NodeMetricRow
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import render_report
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.summary_view import DEMO_GAP
from tests.unit.incident_orchestrator_agent.service.test_report_contract import report


def summary_of(html):return html.split('id="summary"',1)[1].split('</section>',1)[0]


def test_header_contains_actual_title_cluster_and_period():
    html=render_report(replace(report(),cluster='prod-es'))
    assert '<h1>Elasticsearch 쿼리·노드 로그 분석</h1>' in html and 'prod-es' in html
    assert '2026-10-01 09:00:00' in html and '2026-10-01 09:01:00' in html and '생성' in html


def test_demo_is_identified_once():
    html=render_report(report(),gaps=(DEMO_GAP,))
    assert html.count('합성 데이터로 실제 보고서 생성 경로를 실행한 예시입니다.')==1


def test_summary_shows_total_and_peak_from_all_executions():
    summary=summary_of(render_report(report()))
    assert '12건' in summary and '1.96s' in summary and 'search' in summary
    assert '평균' not in summary and 'rejected' not in summary


@pytest.mark.parametrize('status',['NOT_VERIFIED','MISMATCH'])
def test_verification_failure_does_not_promote_headline(status):
    summary=summary_of(render_report(report(status)))
    assert status not in summary and 'unverified conclusion' not in summary
    assert 'class="report-headline"' in summary


def test_counter_is_per_node_cumulative_and_does_not_create_critical():
    r=report()
    nodes=(NodeMetricRow(node='d1',samples=2,search_rejected_max=99),NodeMetricRow(node='ghost',samples=0,search_rejected_max=999))
    html=render_report(replace(r,observations=replace(r.observations,nodes=nodes)))
    assert 'Search rejected 누적' in html and '>99<' in html
    assert 'ghost' not in html and 'CRITICAL' not in summary_of(html)


def test_collection_failure_and_analysis_failure_remain_visible():
    html=render_report(report(),analysis_failed=True,gaps=('SSH 수집 실패',))
    assert 'SSH 수집 실패' in html and '분석 실패' in html
