from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import Observations, SourceWindowStatus
from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource, EvidenceProvenance
from cluster_doctor.incident_orchestrator_agent.model.incident_report import IncidentAnalysisReport, Narrative
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import render_report
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import render_text

T0=datetime(2026,10,1,tzinfo=UTC)

def report(status='NOT_VERIFIED'):
    q=QueryLogEntry(reg_date=T0,host='client',run_time=Decimal('1.96'),success='Y',s_date=1,e_date=2,date_range=2,
        keyword=('a','b','<script>','d','e'),url='POST http://es:9200/index/_search\n{"size":20}',cmd='search',
        service='',env='',project='',company='',user='',search_count=1,etc='',cluster='es')
    logs=tuple(replace(q,run_time=Decimal('1.96')-Decimal(n)/100, cmd='bulk' if n==1 else 'search') for n in range(12))
    e=Evidence(evidence_id='INTERNAL_ID',event_time=T0,source=EvidenceSource.NODE_LOG,node_name='node',message='warn',raw='WARN <stack>&',
        provenance=EvidenceProvenance(method='ssh',file_path='/es/log',host='es'),time_origin='fallback')
    obs=Observations(query_requests=logs,requested=((T0,T0+timedelta(minutes=1)),),source_statuses=(SourceWindowStatus('es_query_log',T0,T0+timedelta(minutes=1),'ok',12,T0),))
    return IncidentAnalysisReport(observations=obs,evidence=(e,),narrative=Narrative(headline='unverified conclusion',root_cause='unverified cause'),verification_status=status)


def test_actual_html_is_individual_source_backed_report():
    html=render_report(report())
    sections=['summary','query-trend','timeline','query-ranking','ssh-logs','system-metrics','causes']
    positions=[html.index(f'id="{name}"') for name in sections]
    assert positions==sorted(positions)
    assert 'Elasticsearch 쿼리·노드 로그 분석' in html and '수집 쿼리 실행 로그' in html
    assert '1.96s' in html and 'bulk' in html
    assert html.count('class="execution-row"')==10
    assert '<svg' in html and 'INTERNAL_ID' not in html and 'href="#evidence' not in html
    assert '평균' not in html and 'Avg' not in html
    assert 'unverified conclusion' not in html and 'unverified cause' not in html
    assert '시각 미확인' in html and '/es/log' in html and 'WARN &lt;stack&gt;&amp;' in html
    assert '<script>' not in html
    text=render_text(report())
    assert '1.96s' in text and 'bulk' in text and 'INTERNAL_ID' not in text
    assert '평균' not in text and 'unverified conclusion' not in text


def test_absent_ssh_does_not_render_empty_table():
    html=render_report(replace(report(),evidence=()))
    assert 'id="ssh-logs"' not in html


def test_global_maximum_nodes_survive_detail_table_limit():
    from cluster_doctor.incident_analysis_agent.model.observations import NodeMetricRow
    r=report()
    nodes=tuple(NodeMetricRow(node=f'busy-{i}',samples=1,search_queue_max=1,cpu_max=5,jvm_heap_max=5) for i in range(10))
    nodes+=(NodeMetricRow(node='hottest-node',samples=1,cpu_max=99,jvm_heap_max=99),)
    r=replace(r,observations=replace(r.observations,nodes=nodes))
    for rendered in (render_report(r),render_text(r)):
        assert 'hottest-node' in rendered and '99%' in rendered


def test_grouped_timeline_preserves_all_supporting_raw_logs_beyond_ssh_limit():
    r=report('PASSED');sample=r.evidence[0]
    evidence=tuple(sample.model_copy(update={'evidence_id':f'E{i}','event_time':T0+timedelta(seconds=i),
        'event_type':'gc','time_origin':'parsed','message':f'GC unique-{i}','raw':f'GC unique-{i}'}) for i in range(11))
    r=replace(r,evidence=evidence)
    for rendered in (render_report(r),render_text(r)):
        assert 'unique-10' in rendered
