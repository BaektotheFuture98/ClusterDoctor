"""Fixed-case protocol checks (offline) and separate actual-model evaluation.

Offline responses are annotated fixtures: they test contracts and calculations,
not a model's ability to diagnose an arbitrary incident.
"""
import argparse
import json
import signal
import sys
from dataclasses import replace
from datetime import datetime, timedelta, UTC
from decimal import Decimal
from itertools import count
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from cluster_doctor.incident_analysis_agent.model.analysis_contract import LogAnalysisRequest
from cluster_doctor.incident_analysis_agent.datasource.clickhouse.query_url import request_fields
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource, EvidenceProvenance
from cluster_doctor.incident_analysis_agent.model.observations import NodeMetricRow, SourceWindowStatus
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport
from cluster_doctor.incident_analysis_agent.service.observation.builder import ObservationBuilder
from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests
from cluster_doctor.incident_analysis_agent.service.report_generation.analysis_context import required_query_evidence
from cluster_doctor.incident_analysis_agent.service.report_generation.report_writer import ReportWriter, build_structured_call
from cluster_doctor.incident_analysis_agent.service.validation.grounding.grounding_validator import GroundingValidator
from cluster_doctor.incident_analysis_agent.service.validation.consistency.report_validation import validate_report

T0=datetime(2026,10,1,tzinfo=UTC)
DEFAULT_CASES=Path(__file__).resolve().parents[1]/'tests/fixtures/report_quality/cases.json'


def inputs(case):
    window=TimeRange(T0,T0+timedelta(minutes=1))
    builder=ObservationBuilder(window)
    provenance=EvidenceProvenance(method='clickhouse',table='fixture.query_logs',query_from=window.start,query_to=window.end)
    requests=[]
    for n,row in enumerate(case.get('requests',[])):
        requests.append(QueryLogEntry(reg_date=T0+timedelta(seconds=n),host='client-host',run_time=Decimal(row.get('run_time','1.96')),
            success='Y',s_date=20260928,e_date=20261001,date_range=4,keyword=tuple(row.get('keyword',['a','b','c','d','e'])),
            keyword_omitted=7,**request_fields(row.get('url','POST http://192.0.2.32:9200/a/_search')),cmd=row.get('cmd','search'),
            service='web',env='fixture',project='fixture',company='fixture',user='fixture',search_count=20,etc='',cluster='fixture',provenance=provenance))
    builder.record_log_observations(requests)
    builder.record_source_status(SourceWindowStatus('es_query_log',window.start,window.end,'failed' if case.get('failed') else 'ok',None if case.get('failed') else len(requests),T0))
    seq=count(1)
    evidence=required_query_evidence(tuple(requests),new_evidence_id=lambda:f'E{next(seq)}')
    if case.get('metric'):
        node=NodeMetricRow(node='metric-node',samples=1,**case['metric'])
        builder.nodes[node.node]=node
        evidence.append(Evidence(evidence_id=f'E{next(seq)}',event_time=T0,source=EvidenceSource.NODE_METRIC,node_name=node.node,
            message=case.get('raw',json.dumps(case['metric'])),provenance=EvidenceProvenance(method='clickhouse',table='fixture.node_metric')))
    if case.get('ssh'):
        ssh=case['ssh']
        evidence.append(Evidence(evidence_id=f'E{next(seq)}',event_time=T0,source=EvidenceSource.NODE_LOG,node_name=ssh['host'],
            message=ssh['raw'],time_origin=ssh['time_origin'],severity='Warning',
            provenance=EvidenceProvenance(method='ssh',host=ssh['host'],file_path='/fixture/elasticsearch.log')))
    if not evidence:
        evidence=[Evidence(evidence_id='E1',event_time=T0,source=EvidenceSource.QUERY_LOG,message=case.get('raw') or 'fetch failed')]
    return window,builder,evidence


def evaluate(*,mode='offline',cases_path=DEFAULT_CASES,env_file=None):
    cases=json.loads(Path(cases_path).read_text())
    call=None
    if mode=='live':
        from cluster_doctor.bootstrap.configuration.settings import Settings
        settings=Settings(_env_file=env_file or '.env')
        call=build_structured_call(provider=settings.llm_provider,model=settings.llm_model,api_key=settings.llm_api_key)
    results=[]
    for case in cases:
        window,builder,evidence=inputs(case)
        obs=builder.to_observations()
        refs=tuple(e.evidence_id for e in evidence)
        if mode=='offline':
            report=LogAnalysisReport(incident_id=case['id'],analyzed_from=window.start,analyzed_to=window.end,
                summary=case['claim'],summary_evidence_refs=refs,evidence_refs=refs)
            response=case.get('response',[{'claim_id':'summary','status':case.get('verdict','MISMATCH'),
                'kind':'analysis_mismatch','reason':'고정 사례의 금지 주장과 원자료 불일치','affected_evidence_refs':list(refs)}])
            checker=GroundingValidator(call_llm=lambda *args, response=response, **kwargs:json.dumps(response))
        else:
            request=LogAnalysisRequest(incident_id=case['id'],cluster='fixture',analysis_window=window,analysis_goal='제공된 실행·노드 로그로 관측과 원인 후보를 분석하라. 근거 없이 원인을 확정하지 마라.')
            draft=ReportWriter(call_llm=call).draft_report(request,evidence,builder)
            report=draft.to_domain(incident_id=case['id'],window=window,evidence_refs=refs)
            checker=GroundingValidator(call_llm=call)
        issues=checker.validate(report,evidence,observations=obs,candidates=obs.candidates)
        kinds={issue.issue_type.value for issue in issues}
        observed='analysis_mismatch' if 'analysis_mismatch' in kinds else ('report_mismatch' if 'report_mismatch' in kinds else ('unverifiable' if issues else 'passed'))
        deterministic=validate_report(report,evidence,candidate_ids={c.candidate_id for c in obs.candidates}) if mode=='live' else None
        ranked=rank_query_requests(obs.query_requests)
        serialized=report.model_dump_json()
        flags=[text for text in case['forbidden'] if text in serialized] if mode=='live' else []
        results.append(dict(id=case['id'],expected=case['expected'],observed=observed,
            contract_pass=observed==case['expected'] if mode=='offline' else None,
            false_pass=bool(case['expected']!='passed' and observed=='passed') if mode=='offline' else bool(flags and observed=='passed'),
            execution_count=len(ranked),maximum_seconds=str(ranked[0].execution_seconds) if ranked and ranked[0].execution_seconds is not None else None,
            ranked_cmds=[r.record.cmd for r in ranked],issues=[i.model_dump(mode='json') for i in issues],
            structural_issues=deterministic.issues if deterministic else [],forbidden_phrase_flags=flags,
            draft=report.model_dump(mode='json') if mode=='live' else None))
        if mode=='live':print(f"completed {case['id']}: {observed}",flush=True)
    return dict(mode=mode,case_count=len(cases),cases=results,contract_failures=sum(r['contract_pass'] is False for r in results),
        false_passes=sum(r['false_pass'] for r in results),live_model_evaluation='executed' if mode=='live' else 'not_run',
        scope='Offline: fixed verdict protocol and observed arithmetic only. Live: one draft and one grounding run per case; phrase flags need human review.')


class EvaluationDeadline(BaseException):
    pass


def main():
    p=argparse.ArgumentParser();p.add_argument('--mode',choices=('offline','live'),default='offline');p.add_argument('--cases',type=Path,default=DEFAULT_CASES);p.add_argument('--output',type=Path,required=True);p.add_argument('--env-file',type=Path);p.add_argument('--budget-seconds',type=int,default=120)
    args=p.parse_args()
    if args.budget_seconds <= 0:p.error('--budget-seconds must be positive')
    if args.mode == 'live' and hasattr(signal, 'SIGALRM'):
        def deadline(*_):raise EvaluationDeadline()
        signal.signal(signal.SIGALRM, deadline)
        signal.alarm(args.budget_seconds)
    try:result=evaluate(mode=args.mode,cases_path=args.cases,env_file=args.env_file)
    except EvaluationDeadline:
        result={'mode':'live','live_model_evaluation':'incomplete','reason':'Evaluation wall-clock budget exceeded; model accuracy is unverified.'}
        args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2));raise SystemExit(1)
    except Exception as exc:
        result={'mode':args.mode,'live_model_evaluation':'failed' if args.mode=='live' else 'not_run','error_type':type(exc).__name__}
        args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2));raise SystemExit(1)
    if hasattr(signal,'SIGALRM'):signal.alarm(0)
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str));print(args.output.resolve())

if __name__=='__main__':main()
