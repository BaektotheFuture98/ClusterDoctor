from dataclasses import replace
from datetime import timedelta
import pytest
from cluster_doctor.incident_analysis_agent.model.observations import NodeMetricRow, SourceWindowStatus
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.report_renderer import render_report
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.report_layout import DEMO_GAP
from tests.unit.incident_orchestrator_agent.service.test_report_contract import report, T0


def section_of(html,name):return html.split(f'id="{name}"',1)[1].split('</section>',1)[0]


def summary_of(html):return ''.join(section_of(html,name) for name in ('overview','incident-analysis'))


def test_overview_contains_title_period_and_generation_time():
    overview=section_of(render_report(report()),'overview')
    assert '<h1>Elasticsearch 클러스터 장애 분석</h1>' in overview
    assert '2026-10-01 09:00:00' in overview and '2026-10-01 09:01:00' in overview and '보고서 생성' in overview
    assert '<h1>장애 제목</h1>' in section_of(render_report(replace(report(),title='장애 제목')),'overview')


def test_demo_is_identified_once():
    html=render_report(report(),gaps=(DEMO_GAP,))
    assert html.count('합성 데이터로 실제 보고서 생성 경로를 실행한 예시입니다.')==1


def test_summary_shows_total_and_peak_from_all_executions():
    html=render_report(report())
    trend=section_of(html,'query-trend')
    assert '<td>12</td>' in trend and '<td>1.96</td>' in trend and '최대 실행시간 1.96s' in section_of(html,'timeline')
    body=html[html.index('<main'):html.index('<details id="evidence">')]
    assert '평균' not in body and 'rejected' not in body


@pytest.mark.parametrize('status',['NOT_VERIFIED','MISMATCH'])
def test_verification_failure_does_not_promote_headline(status):
    summary=summary_of(render_report(report(status)))
    assert status not in summary and 'unverified conclusion' not in summary
    assert 'class="report-headline"' not in summary


def test_counter_is_per_node_cumulative_and_does_not_create_critical():
    r=report()
    nodes=(NodeMetricRow(node='d1',samples=2,search_rejected_max=99),NodeMetricRow(node='ghost',samples=0,search_rejected_max=999))
    html=render_report(replace(r,observations=replace(r.observations,nodes=nodes)))
    assert 'Search rejected 누적' in html and '>99<' in html
    assert 'ghost' not in html and 'CRITICAL' not in summary_of(html)


def test_collection_failure_and_analysis_failure_remain_visible():
    r=replace(report(),evidence=())
    failed=SourceWindowStatus('node_log',T0,T0+timedelta(minutes=1),'failed',None,T0,'ssh refused')
    html=render_report(replace(r,observations=replace(r.observations,source_statuses=(*r.observations.source_statuses,failed))),analysis_failed=True)
    assert 'SSH 수집 실패: ssh refused' in section_of(html,'ssh-logs')
    assert '일부 분석 실패' in section_of(html,'incident-analysis')
