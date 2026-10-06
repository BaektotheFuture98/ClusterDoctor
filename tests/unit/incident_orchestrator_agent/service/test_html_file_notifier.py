from tests.unit.incident_orchestrator_agent.service.test_report_contract import report
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import render_report


def test_report_does_not_include_offender_grouping():
    html=render_report(report())
    assert '가해자 집계' not in html and '느린 개별 실행 로그' in html


def test_ssh_source_is_visible_without_evidence_ids_or_links():
    html=render_report(report())
    assert '/es/log' in html and 'WARN &lt;stack&gt;&amp;' in html
    assert 'INTERNAL_ID' not in html and 'href="#evidence' not in html
