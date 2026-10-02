from datetime import datetime
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.datasource.clickhouse.query_log import entry_from_row
from cluster_doctor.incident_analysis_agent.datasource.clickhouse.query_url import request_fields
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import record_json

ROW = dict(
    reg_date=datetime(2026, 10, 2, 9, 32, 35, tzinfo=KST),
    host='172.16.108.255', run_time=Decimal('2.56'), success='Y',
    s_date=0, e_date=0, date_range=0,
    keyword=['화재', '화재 보험', '삼성화재', '메리츠화재', '현대해상화재', '동부화재', '흥국화재'],
    url=(
        'GET http://192.168.1.29:9200/quetta_realtime/_search?track_total_hits=true\n'
        '{"size":200,"query":{"query_string":{"query":'
        '"((\\"동부화재\\")) AND (inl_spam:(0)) AND (in_trend:(1 OR 2 OR 3))"}}}'
    ),
    cmd='search', service='app', env='prod', project='p', company='c', user='u',
    search_count=0, etc='', cluster='quetta',
)


def test_entry_keeps_only_parsed_request_fields():
    entry = entry_from_row(ROW, None)

    assert entry.keyword == ('화재', '화재 보험', '삼성화재', '메리츠화재', '현대해상화재')
    assert entry.keyword_omitted == 2
    assert entry.target_host == '192.168.1.29'
    assert entry.index_name == 'quetta_realtime'
    assert entry.conditions == ('size=200', 'inl_spam: 1개 조건', 'in_trend: 3개 조건')
    assert not hasattr(entry, 'url')
    assert not hasattr(entry, 'additional_fields')
    assert '동부화재' not in record_json(entry)


def test_unparsable_body_falls_back_to_short_excerpt():
    fields = request_fields('POST http://192.0.2.32:9200/_bulk\n{"index":{}}\n{"a":1}')

    assert fields['target_host'] == '192.0.2.32'
    assert fields['index_name'] is None
    assert fields['conditions'][0].startswith('원문: POST http://192.0.2.32:9200/_bulk')


def test_query_records_carry_record_key():
    from cluster_doctor.incident_analysis_agent.datasource.clickhouse import query_log
    from cluster_doctor.incident_analysis_agent.model.log_entries import query_record_key

    entry = entry_from_row(ROW, None)

    [record] = query_log.to_records([entry])

    assert record.record_key == query_record_key(entry)
