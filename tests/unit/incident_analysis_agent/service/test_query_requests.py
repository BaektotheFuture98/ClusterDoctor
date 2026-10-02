from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.datasource.clickhouse.query_url import request_fields
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.service.observation.compute import slow_candidates, timeline_row

T0 = datetime(2026, 10, 1, tzinfo=UTC)


def query(*, runtime='1.96', url='POST http://192.168.1.32:9200/index/_search', host='211.188.49.31'):
    return QueryLogEntry(reg_date=T0, host=host, run_time=Decimal(runtime), success='Y',
        s_date=20260928, e_date=20261001, date_range=4, keyword=('a','b','c','d','e'),
        **request_fields(url), cmd='search', service='web', env='prod', project='project', company='company',
        user='user', search_count=20, etc='', cluster='es', keyword_omitted=7)


def test_same_five_keywords_keep_distinct_rows():
    from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests
    a = query()
    b = query(url='POST http://192.168.1.33:9200/other/_search')
    rows = rank_query_requests((a, b, a))
    assert len(rows) == 3
    assert rows[0].record.keyword == rows[1].record.keyword
    assert rows[0].record_key != rows[1].record_key
    assert rows[0].ordinal != rows[2].ordinal


def test_invalid_runtime_never_wins():
    from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests
    logs = tuple(query(runtime=v) for v in ('NaN', 'Infinity', '-1', '0', '1.96'))
    rows = rank_query_requests(logs)
    assert [r.execution_seconds for r in rows] == [Decimal('1.96'), Decimal(0), None, None, None]
    assert timeline_row(T0, list(logs)).runtime_max == Decimal('1.96')
    assert slow_candidates(list(logs), limit=1)[0].run_time == Decimal('1.96')


def test_client_host_is_not_es_node():
    candidates = slow_candidates([query()])
    assert candidates[0].node == ''
    assert candidates[0].request_host == '211.188.49.31'
    assert candidates[0].target_host == '192.168.1.32'


def test_different_conditions_do_not_collapse_candidates():
    from cluster_doctor.incident_analysis_agent.service.observation.builder import ObservationBuilder
    from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
    from datetime import timedelta
    b = ObservationBuilder(TimeRange(T0, T0 + timedelta(minutes=1)))
    b.record_candidates([query(), query(url='POST http://192.168.1.33:9200/other/_search')])
    assert len(b.to_observations().candidates) == 2


def test_conditions_use_only_the_records_own_dsl():
    from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests
    raw = 'POST http://192.168.1.32:9200/index/_search\n{"size":20,"track_total_hits":true,"query":{"range":{"in_date":{"gte":20260928,"lte":20261001}}}}'
    row = rank_query_requests((query(url=raw),))[0]
    assert row.index_name == 'index'
    assert any('20260928' in x and '20261001' in x for x in row.conditions)
    assert 'size=20' in row.conditions
    assert 'track_total_hits=true' in row.conditions
    bad = rank_query_requests((query(url='broken\n{'),))[0]
    assert bad.target_host is None
    assert bad.conditions == ()


def test_ambiguous_candidate_is_not_attached():
    from cluster_doctor.incident_analysis_agent.service.observation.query_requests import matching_candidate
    candidates = tuple(slow_candidates([query()]))
    assert matching_candidate(query(), (query(), query()), candidates) is None
    assert matching_candidate(query(), (query(),), candidates) == candidates[0]
