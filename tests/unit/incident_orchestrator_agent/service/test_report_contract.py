from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from cluster_doctor.incident_analysis_agent.datasource.clickhouse.query_url import request_fields
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import Observations, SourceWindowStatus
from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource, EvidenceProvenance
from cluster_doctor.incident_orchestrator_agent.model.incident_report import IncidentAnalysisReport, Narrative
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import render_report
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import render_text

T0=datetime(2026,10,1,tzinfo=UTC)

def report(status='NOT_VERIFIED'):
    q=QueryLogEntry(reg_date=T0,host='client',run_time=Decimal('1.96'),success='Y',s_date=1,e_date=2,date_range=2,
        keyword=('a','b','<script>','d','e'),**request_fields('POST http://es:9200/index/_search\n{"size":20}'),cmd='search',
        service='',env='',project='',company='',user='',search_count=1,etc='',cluster='es')
    logs=tuple(replace(q,run_time=Decimal('1.96')-Decimal(n)/100, cmd='bulk' if n==1 else 'search') for n in range(12))
    e=Evidence(evidence_id='INTERNAL_ID',event_time=T0,source=EvidenceSource.NODE_LOG,node_name='node',message='WARN <stack>&',
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
    assert 'id="ssh-logs"' in html
    section=html.split('id="ssh-logs"')[1].split('</section>')[0]
    assert '<table' not in section
    assert section.endswith('</h2>')


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
        'event_type':'gc','time_origin':'parsed','message':f'GC unique-{i}'}) for i in range(11))
    r=replace(r,evidence=evidence)
    for rendered in (render_report(r),render_text(r)):
        assert 'unique-10' in rendered


def test_unverified_report_has_fact_summary_without_internal_diagnostics():
    r=report()
    html=render_report(r,gaps=('리포트 검증 불일치: summary: internal validator reasoning', 'es_query_log 수집 실패'))
    assert '관측된 실행 로그에서 search의 최대 실행시간은 1.96초입니다.' in html
    assert 'internal validator reasoning' not in html
    assert 'es_query_log 수집 실패' in html
    assert '저장된 키워드:' in html.split('id="query-trend"')[0]
    assert '관측된 실행 로그에서 search의 최대 실행시간은 1.96초입니다.' in render_text(r)
    assert 'unverified conclusion' not in html


def test_timeline_uses_single_observation_without_internal_baseline():
    from importlib.util import spec_from_file_location, module_from_spec
    from pathlib import Path
    spec=spec_from_file_location('report_preview',Path(__file__).resolve().parents[4]/'scripts/preview_report.py')
    module=module_from_spec(spec);spec.loader.exec_module(module)
    r=module.build_example('ssh')
    for rendered in (render_report(r),render_text(r)):
        assert '중앙값' not in rendered
        assert rendered.count('query runtime 지연 피크') <= 1


def test_all_zero_queues_do_not_list_every_node():
    from cluster_doctor.incident_analysis_agent.model.observations import NodeMetricRow
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import system_maxima
    nodes=tuple(NodeMetricRow(node=f'node-{i}',samples=1) for i in range(100))
    lines=system_maxima(nodes)
    assert 'Search 큐 최대 0 · 전체 관측 노드 100개' in lines
    assert 'Write 큐 최대 0 · 전체 관측 노드 100개' in lines


def test_validation_diagnostics_are_not_part_of_operator_report():
    for status in ('PASSED','NOT_VERIFIED','MISMATCH'):
        r=report(status)
        html=render_report(r,gaps=('리포트 검증 불일치: private explanation',))
        text=render_text(r)
        assert 'private explanation' not in html
        assert '분석 해석 검증' not in html and '분석 해석 검증' not in text
        assert '검증을 완료한 원인 판단 없음' not in html


def test_fallback_has_same_summary_and_cause_slots_as_standard_report():
    html=render_report(report())
    summary=html.split('id="summary"')[1].split('</section>')[0]
    causes=html.split('id="causes"')[1].split('</section>')[0]
    assert 'class="report-headline"' in summary
    assert '<h3>' in causes and '확신도:' in causes and '판단 근거' in causes


def test_optional_source_sections_are_fixed_and_empty_without_records():
    r=replace(report(),evidence=())
    html=render_report(r)
    sections=['summary','query-trend','timeline','query-ranking','master-logs','slowlogs','ssh-logs','system-metrics','causes']
    assert [html.index(f'id="{s}"') for s in sections]==sorted(html.index(f'id="{s}"') for s in sections)
    for name in ('master-logs','slowlogs','ssh-logs'):
        body=html.split(f'id="{name}"')[1].split('</section>')[0].split('</h2>')[1]
        assert body==''
    text=render_text(r)
    assert text.index('마스터 노드 로그') < text.index('slowlog 로그') < text.index('SSH 노드 로그')
    assert '로그 없음' not in text


def test_master_observations_survive_without_selected_evidence_or_ssh():
    from cluster_doctor.incident_analysis_agent.model.observations import MasterEvent
    r=replace(report(),evidence=())
    event=MasterEvent(timestamp=T0,node='master',level='WARN',logger='cluster',line='master-only <raw>&',rendered='master-only <raw>&')
    r=replace(r,observations=replace(r.observations,master_events=(event,),master_log_total=1))
    html=render_report(r)
    assert 'master-only &lt;raw&gt;&amp;' in html.split('id="master-logs"')[1].split('</section>')[0]
    assert 'master-only' in html.split('id="timeline"')[1].split('</section>')[0]
    assert 'master-only <raw>&' in render_text(r)
    assert html.split('id="ssh-logs"')[1].split('</section>')[0].endswith('</h2>')


def test_slowlog_and_ssh_do_not_depend_on_master_records():
    r=report();slow=r.evidence[0].model_copy(update={'source':EvidenceSource.SLOWLOG,'message':'slow-only <raw>','time_origin':'parsed'})
    r=replace(r,evidence=(slow,*r.evidence))
    html=render_report(r)
    assert 'slow-only &lt;raw&gt;' in html.split('id="slowlogs"')[1].split('</section>')[0]
    assert 'WARN &lt;stack&gt;&amp;' in html.split('id="ssh-logs"')[1].split('</section>')[0]
    assert html.split('id="master-logs"')[1].split('</section>')[0].endswith('</h2>')


def test_all_optional_source_combinations_keep_slots_and_own_records():
    from itertools import product
    from cluster_doctor.incident_analysis_agent.model.observations import MasterEvent
    for has_master,has_slow,has_ssh in product((False,True),repeat=3):
        r=report();ssh=r.evidence[0]
        slow=ssh.model_copy(update={'source':EvidenceSource.SLOWLOG,'message':'slow-combination'})
        master=MasterEvent(timestamp=T0,node='master',level='WARN',line='master-combination')
        r=replace(r,evidence=tuple(([slow] if has_slow else [])+([ssh] if has_ssh else [])),
            observations=replace(r.observations,master_events=(master,) if has_master else ()))
        html=render_report(r);text=render_text(r)
        for key,phrase,present in [('master-logs','master-combination',has_master),('slowlogs','slow-combination',has_slow),('ssh-logs','WARN &lt;stack&gt;&amp;',has_ssh)]:
            section=html.split(f'id="{key}"')[1].split('</section>')[0]
            assert (phrase in section)==present
            assert '로그 없음' not in section
        assert ('master-combination' in text)==has_master
        assert ('slow-combination' in text)==has_slow


def test_optional_log_limits_and_missing_master_time_are_preserved():
    from cluster_doctor.incident_analysis_agent.model.observations import MasterEvent
    r=report();sample=r.evidence[0]
    events=tuple(MasterEvent(timestamp=None,node='master',line=f'M-{i}') for i in range(121))
    slow=tuple(sample.model_copy(update={'evidence_id':f'S{i}','source':EvidenceSource.SLOWLOG,'message':f'S-{i:02d}'}) for i in range(11))
    r=replace(r,evidence=slow,observations=replace(r.observations,master_events=events))
    html=render_report(r)
    master=html.split('id="master-logs"')[1].split('</section>')[0]
    sl=html.split('id="slowlogs"')[1].split('</section>')[0]
    assert master.count('<pre')==120 and sl.count('<pre')==10
    assert all(f'S-{i:02d}' in sl for i in range(10)) and 'S-10' not in sl
    assert '시각 미확인' in master
    assert 'M-' not in html.split('id="timeline"')[1].split('</section>')[0]
    assert len(r.observations.master_events)==121 and len(r.evidence)==11


def test_failed_collection_keeps_empty_source_slot_and_original_status():
    r=replace(report(),evidence=())
    status=SourceWindowStatus('node_log',T0,T0+timedelta(minutes=1),'failed',None,T0,host='data-node')
    r=replace(r,observations=replace(r.observations,source_statuses=(status,)))
    html=render_report(r)
    assert html.split('id="ssh-logs"')[1].split('</section>')[0].endswith('</h2>')
    assert r.observations.source_statuses[0].status=='failed'
    assert 'data-node 수집 실패' in html


def test_validation_execution_failure_reason_is_not_rendered():
    html=render_report(report(),gaps=('리포트 검증이 실패했다: ValueError', 'SSH 수집 실패'))
    assert 'ValueError' not in html
    assert 'SSH 수집 실패' in html


def test_failed_analysis_plain_fallback_does_not_promote_narrative():
    text=render_text(report('PASSED'),analysis_failed=True)
    assert 'unverified conclusion' not in text
    assert '관측된 실행 로그' in text


def test_selected_master_and_observed_master_are_one_timeline_event():
    from cluster_doctor.incident_analysis_agent.model.observations import MasterEvent
    r=report();line='cluster state publication timed out'
    master=MasterEvent(timestamp=T0,node='master',level='WARN',logger='cluster',line=line)
    evidence=Evidence(evidence_id='selected-master',event_time=T0,source=EvidenceSource.MASTER_LOG,node_name='master',message=line,event_type='cluster_publication',severity='Warning')
    r=replace(r,evidence=(evidence,),observations=replace(r.observations,master_events=(master,)))
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import report_timeline
    items=[text for c in report_timeline(r) for text in (c.representative_event,*(i.text for i in (*c.impacts,*c.causes))) if line in text]
    assert len(items)==1


def test_fallback_narrative_cites_evidence_matching_top_ranked_request_key():
    from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import display_narrative
    r=report('PASSED')
    top=rank_query_requests(r.observations.query_requests)[0]
    other=r.evidence[0]
    matching=other.model_copy(update={'evidence_id':'MATCH','source':EvidenceSource.QUERY_LOG,'record_key':top.record_key})
    r=replace(r,evidence=(other,matching),narrative=Narrative(headline='ok'))
    cited=display_narrative(r,analysis_failed=True).headline_citations
    assert [c.evidence_id for c in cited]==['MATCH']
    assert display_narrative(r).headline=='ok'
