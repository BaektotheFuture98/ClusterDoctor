from dataclasses import replace
from datetime import timedelta

from cluster_doctor.incident_analysis_agent.model.observations import MasterEvent
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection import report_content
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import source_log_sections
from tests.unit.incident_orchestrator_agent.service.test_report_contract import report, T0


def health(i):
    line = f'Cluster health status changed from [RED] to [YELLOW] (reason: [shards started [[idx_{i}][0], [idx_{i}][1]]])'
    return MasterEvent(timestamp=T0 + timedelta(seconds=i), node='m1', level='INFO', logger='o.e.c.r.a.AllocationService', line=line)


def master_lines(events):
    r = report('PASSED')
    r = replace(r, observations=replace(r.observations, master_events=tuple(events)))
    return dict((i, lines) for i, _, lines in source_log_sections(r))['master-logs']


def test_same_shape_lines_collapse_and_warn_comes_first():
    warn = MasterEvent(timestamp=T0 + timedelta(minutes=5), node='m1', level='WARN', logger='o.e.c.r.a.AllocationService',
                       line='[idx_v1][1] marking unavailable shards as stale [abc]')
    lines = master_lines([health(i) for i in range(5)] + [warn])
    assert len(lines) == 2
    assert 'WARN' in lines[0] and '외 ' not in lines[0]
    assert lines[1].endswith('· 외 4건 (' + lines[1].split('(')[-1])
    assert '~' in lines[1].rsplit('(', 1)[1]


def test_distinct_messages_stay_separate_and_error_first():
    err = MasterEvent(timestamp=T0 + timedelta(minutes=1), node='m1', level='ERROR', logger='l', line='boom')
    other = MasterEvent(timestamp=T0, node='m1', level='INFO', logger='l', line='hello')
    lines = master_lines([other, err])
    assert 'ERROR' in lines[0] and 'INFO' in lines[1]


def test_group_lines_are_capped(monkeypatch):
    monkeypatch.setattr(report_content, 'MASTER_DISPLAY_LIMIT', 3)
    events = [MasterEvent(timestamp=T0 + timedelta(seconds=i), node='m', level='INFO', logger='l', line=f'kind{chr(97 + i)}') for i in range(6)]
    assert len(master_lines(events)) == 3
