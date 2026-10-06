import json
import pytest
from datetime import datetime, timedelta, timezone
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport, VerificationStatus
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.service.report_generation.report_writer import ReportWriter

START = datetime(2026, 10, 5, 8, 16, tzinfo=timezone.utc)


def test_reviewed_paragraphs_and_summary_sources_reach_the_rendered_report():
    from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.output_mapping import to_incident_analysis_report
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import render_report
    first = LogAnalysisReport(incident_id='I', analyzed_from=START, analyzed_to=START+timedelta(minutes=1))
    evidence = [Evidence(evidence_id='S1', event_time=START, source=EvidenceSource.QUERY_LOG, message='query-source'),
                Evidence(evidence_id='S2', event_time=START, source=EvidenceSource.MASTER_LOG, message='master-source')]
    draft = {'summary': '초안 요약', 'summary_evidence_refs': ['S1'], 'findings': [],
             'root_causes': [], 'recommendations': [], 'unresolved_questions': []}
    reviewed = {**draft, 'summary': '지연과 관련 로그를 연결하되 원인 연결은 미확인이다.',
                'summary_evidence_refs': ['S1', 'S2'],
                'root_causes': [{'statement': '가능성: 처리 대기', 'confidence': 'Low',
                                'mechanism': '관측을 설명한다.\n\n영향 경로는 조건부다.',
                                'supporting_evidence_refs': ['S2']}],
                'recommendations': [{'text': '대상을 식별하고 대기를 대조한다.', 'cause_index': 0, 'evidence_refs': ['S1']}]}
    responses = iter((json.dumps(draft, ensure_ascii=False), json.dumps(reviewed, ensure_ascii=False)))
    result = ReportWriter(call_llm=lambda *a, **k: next(responses)).draft_incident(
        incident_id='I', cluster='C', window_reports=[first], observations=Observations(), evidence=evidence)
    output = to_incident_analysis_report(result.model_copy(update={'verification_status': VerificationStatus.PASSED}), Observations(), evidence)
    assert output.narrative.headline == '지연과 관련 로그를 연결하되 원인 연결은 미확인이다.'
    assert tuple(c.evidence_id for c in output.narrative.headline_citations) == ('S1', 'S2')
    html = render_report(output)
    assert '초안 요약' not in html
    assert '<p>영향 경로는 조건부다.</p>' in html
    assert 'query-source' in html and 'master-source' in html


@pytest.mark.parametrize('status', [VerificationStatus.PASSED, VerificationStatus.MISMATCH, VerificationStatus.NOT_VERIFIED])
def test_final_validation_reasons_remain_internal_across_delivery(monkeypatch, status):
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock
    from cluster_doctor.incident_orchestrator_agent.agent.adapter import _DeepAgentIncidentAnalyzer
    from cluster_doctor.incident_orchestrator_agent.model.incident import Incident, IncidentStatus
    from cluster_doctor.incident_orchestrator_agent.model.lifecycle import IncidentAnalysisRequest
    from cluster_doctor.incident_orchestrator_agent.model.report_delivery import ReportPublication
    from cluster_doctor.incident_orchestrator_agent.model.window_result import WindowResult
    from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
    from cluster_doctor.incident_orchestrator_agent.service.incident_lifecycle.analyze_incident import AnalyzeIncident
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import render_report
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import render_text
    first = LogAnalysisReport(incident_id='I', analyzed_from=START, analyzed_to=START+timedelta(minutes=1))
    issues = () if status is VerificationStatus.PASSED else ('internal final reasoning',)
    final = first.model_copy(update={'summary': '최종 서술', 'verification_status': status, 'verification_issues': issues})
    analyzer = object.__new__(_DeepAgentIncidentAnalyzer)
    analyzer._seams = SimpleNamespace()
    analyzer._recursion_limit = 40
    def stream(state, *a, **k):
        yield {**state, 'window_results': (WindowResult(window=TimeRange(START, first.analyzed_to), report=first),),
               'status': IncidentStatus.COMPLETED, 'closing_reason': '완료',
               'accumulated_gaps': ('SSH 수집 실패',), 'latest_verification_status': VerificationStatus.PASSED}
    analyzer._compile = lambda _: SimpleNamespace(stream=stream)
    monkeypatch.setattr('cluster_doctor.incident_analysis_agent.service.report_generation.incident_synthesis.synthesize_incident', lambda **k: final)
    incident = Incident(incident_id='I', cluster='C', trigger_time=START, kafka_receive_time=START)
    result = analyzer.analyze(IncidentAnalysisRequest(incident, START, first.analyzed_to, 0))
    assert result.report.verification_issues == issues
    assert result.report.verification_status is status
    assert result.failed is (status is VerificationStatus.MISMATCH)
    assert result.gaps == ('SSH 수집 실패',)
    publication = []
    async def publish(report, *, gaps, analysis_failed):
        publication.append(render_report(report, gaps=gaps, analysis_failed=analysis_failed))
        publication.append(render_text(report, analysis_failed=analysis_failed))
        return ReportPublication()
    publisher = SimpleNamespace(publish=AsyncMock(side_effect=publish))
    service = AnalyzeIncident(incident_analyzer=MagicMock(), report_publisher=publisher)
    asyncio.run(service._deliver(result, result.failed, result.gaps))
    html, text = publication
    assert 'internal final reasoning' not in html and 'internal final reasoning' not in text
    assert 'SSH 수집 실패' in html


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
    assert START.isoformat() in calls[0] and last.analyzed_to.isoformat() in calls[0]
    assert '마지막 구간 정상' not in calls[0]
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


def test_incident_uses_minimal_json_schema_transport_with_explicit_contract():
    seen=[]
    first=LogAnalysisReport(incident_id='I',analyzed_from=START,analyzed_to=START+timedelta(minutes=1))
    def call(messages,**kwargs):
        seen.append(('\n'.join(m['content'] for m in messages),kwargs,messages))
        return '{"summary":"종합","findings":[],"root_causes":[],"recommendations":[],"unresolved_questions":[]}'
    ReportWriter(call_llm=call).draft_incident(incident_id='I',cluster='C',window_reports=[first],observations=Observations(),evidence=[])
    format=seen[0][1]['response_format']
    assert format['type']=='json_schema'
    assert format['json_schema']['schema']=={'type':'object','additionalProperties':True}
    assert '"cause_index"' in seen[0][0] and '"summary_evidence_refs"' in seen[0][0]
    assert [m['role'] for m in seen[0][2]]==['system','user']
    assert '누적 카운터' in seen[0][2][0]['content']


def test_final_prompt_has_no_window_expansion_instructions():
    first=LogAnalysisReport(incident_id='I',analyzed_from=START,analyzed_to=START+timedelta(minutes=1),summary='앞선 판단은 참고')
    seen=[]
    def call(messages,**kwargs):
        seen.extend(messages)
        return '{"summary":"최종 요약","findings":[],"root_causes":[],"recommendations":[],"unresolved_questions":[]}'
    ReportWriter(call_llm=call).draft_incident(incident_id='I',cluster='C',window_reports=[first],observations=Observations(),evidence=[])
    system=seen[0]['content']
    assert 'needs_more_context' not in system
    assert 'suggested_windows' not in system
    assert 'Cross-source Analysis' not in system
    assert '앞선 판단은 참고' not in seen[1]['content']
    assert '2026-10-05T17:16:00+09:00' in seen[1]['content']


def test_final_revision_keeps_final_instruction_role_and_contract():
    first=LogAnalysisReport(incident_id='I',analyzed_from=START,analyzed_to=START+timedelta(minutes=1),summary='보존할 사실')
    seen=[]
    def call(messages,**kwargs):
        seen.append((messages,kwargs))
        return '{"summary":"교정한 사실","findings":[],"root_causes":[],"recommendations":[],"unresolved_questions":[]}'
    report=ReportWriter(call_llm=call).revise_report(first,('인용한 시각이 다름',),[],incident_final=True)
    assert report.summary=='교정한 사실'
    messages,kwargs=seen[0]
    assert [m['role'] for m in messages]==['system','user']
    assert 'needs_more_context' not in '\n'.join(m['content'] for m in messages)
    assert '보존할 사실' in messages[1]['content'] and '인용한 시각이 다름' in messages[1]['content']
    assert kwargs['response_format']['type']=='json_schema'


def test_final_input_retains_facts_without_prior_interpretation():
    from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
    from cluster_doctor.incident_analysis_agent.model.report import RootCause
    prior=LogAnalysisReport(incident_id='I',analyzed_from=START,analyzed_to=START+timedelta(minutes=1),
        summary='이전 요약의 미확인 변화 주장',
        root_causes=(RootCause(statement='이전 원인의 확정 주장',confidence='Medium'),),
        unresolved_questions=('이전 가설을 전제한 질문',),verification_status=VerificationStatus.PASSED)
    fact=Evidence(evidence_id='E-real',event_time=START,source=EvidenceSource.QUERY_LOG,message='보존할 관측 사실')
    seen=[]
    def call(messages,**kwargs):
        seen.extend(messages)
        return '{"summary":"종합","findings":[],"root_causes":[],"recommendations":[],"unresolved_questions":[]}'
    ReportWriter(call_llm=call).draft_incident(incident_id='I',cluster='C',window_reports=[prior],observations=Observations(),evidence=[fact])
    user=seen[1]['content']
    assert '보존할 관측 사실' in user and 'E-real' in user
    assert prior.analyzed_to.isoformat() in user
    assert not any(text in user for text in (prior.summary,prior.root_causes[0].statement,prior.unresolved_questions[0],'verification_status'))


def test_final_diagnosis_uses_one_editor_pass_with_facts_and_draft():
    from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
    first=LogAnalysisReport(incident_id='I',analyzed_from=START,analyzed_to=START+timedelta(minutes=1))
    fact=Evidence(evidence_id='E-real',event_time=START,source=EvidenceSource.QUERY_LOG,message='보존할 관측')
    seen=[]
    def call(messages,**kwargs):
        seen.append((messages,kwargs))
        summary='작성한 초안' if len(seen)==1 else '교정한 관측'
        return json.dumps(dict(summary=summary,findings=[],root_causes=[],recommendations=[],unresolved_questions=[]),ensure_ascii=False)
    result=ReportWriter(call_llm=call).draft_incident(incident_id='I',cluster='C',window_reports=[first],observations=Observations(),evidence=[fact])
    assert result.summary=='교정한 관측'
    assert len(seen)==2
    messages,kwargs=seen[1]
    assert [m['role'] for m in messages]==['system','user']
    assert '편집자' in messages[0]['content']
    assert '작성한 초안' in messages[1]['content']
    assert '보존할 관측' in messages[1]['content'] and 'E-real' in messages[1]['content']
    assert kwargs['response_format']['type']=='json_schema'


def test_invalid_editor_response_does_not_publish_draft_as_reviewed():
    import pytest
    from pydantic import ValidationError
    first=LogAnalysisReport(incident_id='I',analyzed_from=START,analyzed_to=START+timedelta(minutes=1))
    replies=iter(['{"summary":"작성한 초안","findings":[],"root_causes":[],"recommendations":[],"unresolved_questions":[]}','not json'])
    with pytest.raises(ValidationError):
        ReportWriter(call_llm=lambda *a,**k:next(replies)).draft_incident(incident_id='I',cluster='C',window_reports=[first],observations=Observations(),evidence=[])
