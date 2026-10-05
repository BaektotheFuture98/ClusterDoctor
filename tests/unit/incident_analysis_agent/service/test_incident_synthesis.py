import json
from datetime import datetime, timedelta, timezone
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport, VerificationStatus
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.service.report_generation.report_writer import ReportWriter

START = datetime(2026, 10, 5, 8, 16, tzinfo=timezone.utc)

def test_whole_incident_preserves_early_peak_across_longer_than_ten_minutes():
    first = LogAnalysisReport(incident_id='I', analyzed_from=START, analyzed_to=START+timedelta(minutes=10), summary='초기 17.6초 지연')
    last = first.model_copy(update={'analyzed_from':first.analyzed_to,'analyzed_to':START+timedelta(minutes=12),'summary':'마지막 구간 정상'})
    calls=[]
    def call(messages, **kwargs):
        calls.append('\n'.join(m['content'] for m in messages))
        return json.dumps({'summary':'초기 지연 후 정상화','findings':[],'unresolved_questions':[],'root_causes':[{'statement':'검색 처리 병목 후보','mechanism':'검색 처리 대기로 실행시간이 길어질 수 있음','uncertainties':['대상 노드 연결 미확인']}], 'recommendations':[{'text':'피크 실행 대상의 큐를 확인','cause_index':0}]},ensure_ascii=False)
    report=ReportWriter(call_llm=call).draft_incident(incident_id='I',cluster='C',window_reports=[first,last],observations=Observations(requested=((START,last.analyzed_to),)),evidence=[])
    assert report.analyzed_from==START
    assert report.analyzed_to==last.analyzed_to
    assert '초기 17.6초 지연' in calls[0] and '마지막 구간 정상' in calls[0]
    assert report.root_causes[0].mechanism
    assert report.root_causes[0].uncertainties==('대상 노드 연결 미확인',)
    assert report.recommendations[0].cause_index==0


def test_invalid_incident_response_does_not_become_empty_success():
    import pytest
    first=LogAnalysisReport(incident_id='I',analyzed_from=START,analyzed_to=START+timedelta(minutes=1))
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        ReportWriter(call_llm=lambda *a,**k:'not json').draft_incident(incident_id='I',cluster='C',window_reports=[first],observations=Observations(),evidence=[])


def test_synthesis_validates_new_claims_and_does_not_collect():
    from types import SimpleNamespace
    from cluster_doctor.incident_analysis_agent.service.report_generation.incident_synthesis import synthesize_incident
    first=LogAnalysisReport(incident_id='I',analyzed_from=START,analyzed_to=START+timedelta(minutes=1))
    seen=[]
    def call(messages, **kwargs):
        if kwargs.get('response_format'):
            return json.dumps({'summary':'사건 한국어 요약','findings':[],'unresolved_questions':[],'root_causes':[{'statement':'후보','mechanism':'처리 대기 가능성','uncertainties':['노드 연결 미확인']}], 'recommendations':[{'text':'대상 확인','cause_index':0}]},ensure_ascii=False)
        claims=json.loads(messages[0]['content'].split('Claims:\n')[1].split('\nEvidence:\n')[0])
        seen.extend(c['claim_id'] for c in claims)
        return json.dumps([{'claim_id':c['claim_id'],'status':'PASSED','affected_evidence_refs':[]} for c in claims])
    result=synthesize_incident(seams=SimpleNamespace(report_writer=ReportWriter(call_llm=call),call_llm=call),incident=SimpleNamespace(incident_id='I',cluster='C'),window_reports=[first],observations=Observations(),evidence=[])
    assert result.verification_status is VerificationStatus.PASSED
    assert 'cause:0:mechanism' in seen and 'cause:0:uncertainty:0' in seen


def test_projection_and_html_keep_actions_with_their_cause():
    from cluster_doctor.incident_analysis_agent.service.report_generation.schema import DraftReport
    from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.output_mapping import to_incident_analysis_report
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import render_report
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import render_text
    report=DraftReport(summary='요약',root_causes=[{'statement':'첫 후보','mechanism':'처리 대기 가능성','uncertainties':['연결 미확인']},{'statement':'둘째 후보'}],recommendations=[{'text':'첫 후보 조사','cause_index':0},{'text':'둘째 후보 조사','cause_index':1},{'text':'공통 조사'}]).to_domain(incident_id='I',window=TimeRange(START,START+timedelta(minutes=1)),evidence_refs=()).model_copy(update={'verification_status':VerificationStatus.PASSED})
    output=to_incident_analysis_report(report,Observations(),[])
    html=render_report(output)
    assert html.index('첫 후보 조사') < html.index('둘째 후보') < html.index('둘째 후보 조사')
    assert html.count('첫 후보 조사')==1 and '처리 대기 가능성' in html and '연결 미확인' in html
    text=render_text(output)
    assert text.index('첫 후보 조사')<text.index('둘째 후보')


def test_unknown_action_link_is_a_structural_error():
    from cluster_doctor.incident_analysis_agent.model.report import ReportRecommendation
    from cluster_doctor.incident_analysis_agent.service.validation.consistency.report_validation import validate_report
    report=LogAnalysisReport(incident_id='I',analyzed_from=START,analyzed_to=START+timedelta(minutes=1),recommendations=(ReportRecommendation(text='조사',cause_index=0),))
    assert validate_report(report,[]).issues


def test_malformed_revision_retains_original_report():
    first=LogAnalysisReport(incident_id='I',analyzed_from=START,analyzed_to=START+timedelta(minutes=1),summary='보존할 사건 판단')
    writer=ReportWriter(call_llm=lambda *a,**k:'not json')
    assert writer.revise_report(first,('수정 요청',),[],observations=Observations()) is None


def test_incident_prompt_is_bounded_with_explicit_omissions():
    from cluster_doctor.incident_analysis_agent.model.report import RootCause
    huge=LogAnalysisReport(incident_id='I',analyzed_from=START,analyzed_to=START+timedelta(minutes=1),summary='긴 요약'*10000,root_causes=(RootCause(statement='긴 원인'*10000),))
    calls=[]
    def call(messages,**kwargs):
        calls.append('\n'.join(m['content'] for m in messages))
        return '{"summary":"종합","findings":[],"root_causes":[],"recommendations":[],"unresolved_questions":[]}'
    ReportWriter(call_llm=call).draft_incident(incident_id='I',cluster='C',window_reports=[huge]*12,observations=Observations(),evidence=[])
    assert len(calls[0])<=60000
    assert 'omissions' in calls[0]


def test_analyzer_delivers_synthesis_instead_of_last_window(monkeypatch):
    from types import SimpleNamespace
    from cluster_doctor.incident_orchestrator_agent.agent.adapter import _DeepAgentIncidentAnalyzer
    from cluster_doctor.incident_orchestrator_agent.model.incident import Incident, IncidentStatus
    from cluster_doctor.incident_orchestrator_agent.model.lifecycle import IncidentAnalysisRequest
    from cluster_doctor.incident_orchestrator_agent.model.window_result import WindowResult
    from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
    first=LogAnalysisReport(incident_id='I',analyzed_from=START,analyzed_to=START+timedelta(minutes=5),summary='초기 지연')
    last=first.model_copy(update={'analyzed_from':first.analyzed_to,'analyzed_to':START+timedelta(minutes=6),'summary':'마지막 정상'})
    whole=first.model_copy(update={'analyzed_to':last.analyzed_to,'summary':'초기 장애 후 정상 구간','verification_status':VerificationStatus.PASSED})
    analyzer=object.__new__(_DeepAgentIncidentAnalyzer)
    analyzer._seams=SimpleNamespace()
    analyzer._recursion_limit=40
    windows=(WindowResult(window=TimeRange(first.analyzed_from,first.analyzed_to),report=first),WindowResult(window=TimeRange(last.analyzed_from,last.analyzed_to),report=last))
    def stream(state,*a,**k):
        yield {**state,'window_results':windows,'status':IncidentStatus.COMPLETED,'closing_reason':'완료',
               'latest_verification_status':VerificationStatus.MISMATCH}
    analyzer._compile=lambda _:SimpleNamespace(stream=stream)
    def synthesis(**kwargs):
        assert [r.summary for r in kwargs['window_reports']]==['초기 지연','마지막 정상']
        return whole
    monkeypatch.setattr('cluster_doctor.incident_analysis_agent.service.report_generation.incident_synthesis.synthesize_incident',synthesis)
    incident=Incident(incident_id='I',cluster='C',trigger_time=START,kafka_receive_time=START)
    request=IncidentAnalysisRequest(incident,START,last.analyzed_to,0)
    result=analyzer.analyze(request)
    assert result.report==whole
    assert not result.failed
    def fail(**kwargs): raise ValueError('invalid summary')
    monkeypatch.setattr('cluster_doctor.incident_analysis_agent.service.report_generation.incident_synthesis.synthesize_incident',fail)
    failed=analyzer.analyze(request)
    assert failed.report is None and failed.failed
    assert failed.status is IncidentStatus.FAILED


def test_delivery_dto_keeps_existing_positional_citations():
    from cluster_doctor.incident_orchestrator_agent.model.incident_report import Recommendation, CauseAssessment
    assert Recommendation('확인',()).citations==()
    assert Recommendation('확인',()).cause_index is None
    assert CauseAssessment('후보','Low',(),()).supporting==()


def test_incident_uses_json_object_transport_with_explicit_contract():
    seen=[]
    first=LogAnalysisReport(incident_id='I',analyzed_from=START,analyzed_to=START+timedelta(minutes=1))
    def call(messages,**kwargs):
        seen.append(('\n'.join(m['content'] for m in messages),kwargs,messages))
        return '{"summary":"종합","findings":[],"root_causes":[],"recommendations":[],"unresolved_questions":[]}'
    ReportWriter(call_llm=call).draft_incident(incident_id='I',cluster='C',window_reports=[first],observations=Observations(),evidence=[])
    assert seen[0][1]['response_format']=={'type':'json_object'}
    assert '"cause_index"' in seen[0][0] and '"summary_evidence_refs"' in seen[0][0]
    assert [m['role'] for m in seen[0][2]]==['system','user']
    assert '누적 카운터' in seen[0][2][0]['content']
