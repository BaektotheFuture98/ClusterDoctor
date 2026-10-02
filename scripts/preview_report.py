"""Synthetic DTOs and annotated analysis through the actual mapping/publisher."""
import argparse
import asyncio
import sys
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from itertools import count
from pathlib import Path
from tempfile import TemporaryDirectory
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))

from cluster_doctor.incident_analysis_agent.datasource.clickhouse.query_url import request_fields
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import NodeMetricRow, SourceWindowStatus, MasterEvent
from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource, EvidenceProvenance
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport, RootCause, ReportRecommendation, VerificationStatus
from cluster_doctor.incident_analysis_agent.service.observation.builder import ObservationBuilder
from cluster_doctor.incident_analysis_agent.service.report_generation.analysis_context import required_query_evidence
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.output_mapping import to_incident_analysis_report
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import HtmlFileReportPublisher
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.summary_view import DEMO_GAP
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import render_text

VARIANTS={'default':VerificationStatus.PASSED,'ssh':VerificationStatus.PASSED,'mismatch':VerificationStatus.MISMATCH,'not-verified':VerificationStatus.NOT_VERIFIED,'ssh-not-verified':VerificationStatus.NOT_VERIFIED,'master':VerificationStatus.PASSED,'all':VerificationStatus.PASSED}


def build_example(variant='default'):
    start=datetime(2026,10,1,14,2,tzinfo=KST);window=TimeRange(start,start+timedelta(minutes=5))
    provenance=EvidenceProvenance(method='clickhouse',table='demo.es_query_log',query_from=window.start,query_to=window.end,collected_at=window.end)
    base=QueryLogEntry(reg_date=start+timedelta(seconds=12,microseconds=345000),host='192.0.2.11',run_time=Decimal('1.96'),success='Y',
        s_date=20260928,e_date=20261001,date_range=4,keyword=('반도체','수출','환율','전망','기업'),keyword_omitted=7,
        **request_fields('POST http://192.0.2.32:9200/news/_search\n{"size":20,"query":{"range":{"in_date":{"gte":20260928,"lte":20261001}}}}'),
        cmd='search',service='web',env='demo',project='demo',company='예시 회사',user='예시 사용자',search_count=20,etc='',cluster='demo-es',provenance=provenance)
    requests=[base,replace(base,reg_date=start+timedelta(seconds=25),run_time=Decimal('1.94'),cmd='bulk',keyword=(),keyword_omitted=0,
        **request_fields('POST http://192.0.2.32:9200/_bulk\n{"index":{"_index":"news"}}')),
        replace(base,reg_date=start+timedelta(minutes=1,seconds=17),run_time=Decimal('1.51'),**request_fields('POST http://192.0.2.33:9200/archive/_search\n{"size":500,"track_total_hits":true}'))]
    requests += [replace(base,reg_date=start+timedelta(minutes=m,seconds=30),run_time=Decimal(value),keyword=('환율',),keyword_omitted=0) for m,value in ((1,'0.82'),(2,'0.54'),(4,'0.78'))]
    builder=ObservationBuilder(window);builder.record_log_observations(requests)
    for minute in range(5):
        left=start+timedelta(minutes=minute);records=sum(left<=r.timestamp<left+timedelta(minutes=1) for r in requests)
        builder.record_source_status(SourceWindowStatus('es_query_log',left,left+timedelta(minutes=1),'ok',records,window.end))
    builder.nodes['data-32']=NodeMetricRow(node='data-32',samples=5,cpu_max=47,jvm_heap_max=78,search_queue_max=12,write_queue_max=4,search_rejected_max=8391)
    seq=count(1);evidence=required_query_evidence(tuple(requests),new_evidence_id=lambda:f'E-demo-{next(seq)}')
    query_refs=tuple(e.evidence_id for e in evidence)
    if variant in ('ssh','ssh-not-verified','all'):
        evidence.append(Evidence(evidence_id='E-demo-ssh',event_time=start+timedelta(minutes=1,microseconds=123456),source=EvidenceSource.NODE_LOG,
            node_name='data-99',severity='Warning',message='[2026-10-01T14:03:00.123456+09:00][WARN][JvmGcMonitorService] [data-99] GC overhead: spent [500ms] collecting in the last [1s]',
            provenance=EvidenceProvenance(method='ssh',host='192.0.2.99',file_path='/var/log/elasticsearch/demo-es.log',query_from=window.start,query_to=window.end,collected_at=window.end)))
        evidence.append(Evidence(evidence_id='E-demo-stack',event_time=start+timedelta(minutes=1),source=EvidenceSource.NODE_LOG,
            node_name='data-99',time_origin='inherited',message='    at example.search.QueryPhase.execute(QueryPhase.java:42)\n    at example.search.SearchService.run(SearchService.java:87)',provenance=evidence[-1].provenance))
    if variant in ('master','all'):
        builder.master_logs['preview']=MasterEvent(timestamp=start+timedelta(minutes=1),node='master-demo',level='WARN',logger='cluster',line='cluster state publication timed out',rendered='cluster state publication timed out')
    if variant=='all':
        evidence.append(Evidence(evidence_id='E-demo-slow',event_time=start+timedelta(minutes=1,seconds=10),source=EvidenceSource.SLOWLOG,
            message='[2026-10-01T14:03:10+09:00][WARN][index.search.slowlog.query] took[1500ms], source[{"size":500}]',
            provenance=EvidenceProvenance(method='clickhouse',table='demo.slowlog',query_from=window.start,query_to=window.end)))
    report=LogAnalysisReport(incident_id='DEMO',analyzed_from=window.start,analyzed_to=window.end,
        summary='수집 실행 로그에서 search 1.96초와 bulk 1.94초가 관측됐습니다.',summary_evidence_refs=query_refs[:2],
        root_causes=(RootCause(statement='수집된 실행 기록만으로 지연 원인을 확정할 수 없습니다.',confidence='Low',supporting_evidence_refs=query_refs[:2]),),
        recommendations=(ReportRecommendation(text='1.96초 search의 대상과 조건을 확인하고 해당 대상 노드의 같은 시각 로그를 비교합니다.',evidence_refs=query_refs[:1]),),
        evidence_refs=tuple(e.evidence_id for e in evidence),verification_status=VARIANTS[variant])
    return to_incident_analysis_report(report,builder.to_observations(),evidence,cluster='demo-es')


def main(variant='default',output=None):
    output=Path(output or f'reports/preview-{variant}.html');output.parent.mkdir(parents=True,exist_ok=True)
    report=build_example(variant)
    with TemporaryDirectory(prefix='clusterdoctor-preview-') as directory:
        asyncio.run(HtmlFileReportPublisher(directory).publish(report,gaps=(DEMO_GAP,)))
        files=list(Path(directory).glob('*.html'))
        if len(files)!=1:raise RuntimeError('Actual publisher did not create one HTML report')
        output.write_text(files[0].read_text(),encoding='utf-8')
    output.with_suffix('.txt').write_text(render_text(report),encoding='utf-8')
    print(output.resolve())

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--variant',choices=tuple(VARIANTS),default='default');parser.add_argument('--output',type=Path)
    args=parser.parse_args();main(args.variant,args.output)
