from dataclasses import replace
from datetime import timedelta

from cluster_doctor.incident_analysis_agent.model.observations import MasterEvent, SourceWindowStatus
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import render_report
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import render_text
from test_report_contract import report, T0

END = T0 + timedelta(minutes=1)


def st(source, state, rows=None, error=""):
    return SourceWindowStatus(source, T0, END, state, rows, T0, error)


def make(statuses=(), *, evidence=(), queries=None, master=()):
    r = report('PASSED')
    obs = replace(r.observations, source_statuses=tuple(statuses), master_events=master,
                  **({} if queries is None else {'query_requests': queries}))
    return replace(r, observations=obs, evidence=evidence)


def section(html, name):
    return html.split(f'id="{name}"', 1)[1].split('</section>', 1)[0]


def note(text):
    return f'<p class="hint">{text}</p>'


def master_event():
    return MasterEvent(timestamp=T0, node='master-01', level='WARN', logger='l', line='master line')


def slow_evidence():
    from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
    return Evidence(evidence_id='S1', event_time=T0, source=EvidenceSource.SLOWLOG, message='slow body')


def check(r, name, message):
    assert note(message) in section(render_report(r), name), message
    assert message in render_text(r), message


def test_master_messages():
    check(make([st('master_log', 'ok', 0)], evidence=()), 'master-logs', '특이사항 없음')
    check(make([st('master_log', 'failed', None, 'ch down')]), 'master-logs', '수집 실패: ch down')
    check(make(), 'master-logs', '수집 상태 미확인')


def test_master_note_absent_when_events_exist():
    r = make([st('master_log', 'ok', 1)], master=(master_event(),))
    html = render_report(r)
    assert 'master line' in section(html, 'master-logs') and '<p class="hint">' not in section(html, 'master-logs')


def test_slowlog_messages():
    check(make([st('slowlog', 'ok', 0)]), 'slowlogs', '특이사항 없음')
    check(make([st('slowlog', 'ok', 7)]), 'slowlogs', '수집 7건 중 선별된 항목 없음')
    check(make([st('slowlog', 'failed', None, 'boom')]), 'slowlogs', '수집 실패: boom')
    check(make(), 'slowlogs', '수집 상태 미확인')


def test_slowlog_note_absent_when_evidence_exists():
    html = render_report(make([st('slowlog', 'ok', 7)], evidence=(slow_evidence(),)))
    assert 'slow body' in section(html, 'slowlogs') and 'hint' not in section(html, 'slowlogs')


def ssh_empty(statuses):
    return make(statuses, evidence=())


def test_ssh_messages():
    skipped = st('node_log', 'skipped', None, '마스터 로그에서 조사 대상 노드가 없다')
    check(ssh_empty([skipped, st('master_log', 'ok', 0)]), 'ssh-logs', '마스터 로그에서 조사 대상 노드가 없어 수집하지 않음')
    check(ssh_empty([skipped]), 'ssh-logs', '마스터 로그에서 조사 대상 노드가 없어 수집하지 않음')
    check(ssh_empty([skipped, st('master_log', 'failed', None, 'x')]), 'ssh-logs', '마스터 로그 수집 실패로 조사하지 않음')
    check(ssh_empty([st('node_log', 'failed', None, 'ssh refused')]), 'ssh-logs', 'SSH 수집 실패: ssh refused')
    check(ssh_empty([st('node_log', 'ok', 0)]), 'ssh-logs', '해당 구간 로그 없음')
    check(ssh_empty([st('node_log', 'limited', 42)]), 'ssh-logs', '수집 42줄 중 선별된 항목 없음')
    check(ssh_empty([]), 'ssh-logs', '수집 상태 미확인')


def test_ssh_note_absent_when_evidence_exists():
    html = render_report(make([st('node_log', 'ok', 5)], evidence=report('PASSED').evidence))
    assert 'hint' not in section(html, 'ssh-logs')


def test_empty_query_table_messages():
    failed = make([st('es_query_log', 'failed', None, 'ch timeout')], queries=())
    check(failed, 'query-ranking', '수집 실패: ch timeout')
    for statuses in ([], [st('es_query_log', 'ok', 0)]):
        check(make(statuses, queries=()), 'query-ranking', '쿼리 실행 로그가 수집되지 않았습니다. 수집 상태를 확인하세요')
    assert '수집된 실행 로그 없음' not in render_report(make([], queries=()))


def test_query_table_has_no_note_when_rows_exist():
    html = render_report(make([st('es_query_log', 'ok', 12)]))
    assert 'hint' not in section(html, 'query-ranking')
