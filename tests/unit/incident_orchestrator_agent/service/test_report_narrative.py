"""Narrative ordering and citation preservation in the actual renderers."""
from dataclasses import replace
from datetime import timedelta

import pytest

from cluster_doctor.incident_analysis_agent.model.observations import SourceWindowStatus
from cluster_doctor.incident_orchestrator_agent.model.evidence_citation import EvidenceCitation
from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    CauseAssessment, Finding, Narrative, Recommendation,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.report_renderer import render_report
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import render_text
from tests.unit.incident_orchestrator_agent.service.test_report_contract import report, T0


def section(html, name):
    return html.split(f'id="{name}"', 1)[1].split('</section>', 1)[0]


def collected(html):
    return html[html.index('<details id="evidence">'):]


def example():
    base = report('PASSED')
    evidence = base.evidence[0].model_copy(update={
        'message': 'support-source', 'time_origin': 'parsed',
    })
    citation = EvidenceCitation(evidence.evidence_id, evidence)
    narrative = Narrative(
        causes=(CauseAssessment(
            statement='가능성: 처리 대기',
            mechanism='관측을 설명하는 첫 문단.\n\n조건부 영향 경로 <tag>&.',
            supporting=(citation,),
        ),),
        recommendations=(Recommendation(
            text='요청 대상을 먼저 식별한다. 대기와 요청 지연을 대조한다.',
            cause_index=0, citations=(citation,),
        ),),
    )
    return replace(base, narrative=narrative, evidence=(evidence,))


def test_cause_explanation_and_action_precede_deduplicated_source():
    result = example()
    html = render_report(result)
    analysis, actions = section(html, 'incident-analysis'), section(html, 'recommendations')
    assert '<pre' not in analysis and '<pre' not in actions
    assert html.index('관측을 설명하는 첫 문단.') < html.index('요청 대상을 먼저 식별한다.') < html.index('<details id="evidence">')
    assert '<li>요청 대상을 먼저 식별한다. 대기와 요청 지연을 대조한다.</li>' in actions
    assert 'support-source' not in analysis and 'support-source' not in actions
    assert collected(html).count('support-source') == 1
    assert '<p>조건부 영향 경로 &lt;tag&gt;&amp;.</p>' in analysis
    text = render_text(result)
    summary_text = text.split('핵심 요약', 1)[1].split('쿼리 실행 추이', 1)[0]
    cause_text = text.split('결론', 1)[1].split('핵심 요약', 1)[0]
    assert 'support-source' not in summary_text
    assert cause_text.index('관측을 설명하는 첫 문단.') < cause_text.index('support-source') < cause_text.index('요청 대상을 먼저 식별한다.')
    assert cause_text.count('support-source') == 1
    assert '첫 문단.\n\n조건부 영향 경로 <tag>&.' in cause_text


def test_same_source_is_preserved_for_distinct_causes_and_counter_evidence():
    base = example()
    citation = base.narrative.causes[0].supporting[0]
    first = replace(base.narrative.causes[0], contradicting=(citation,))
    second = CauseAssessment(statement='둘째 후보', mechanism='둘째 설명', supporting=(citation,))
    result = replace(base, narrative=replace(base.narrative, causes=(first, second)))
    html = render_report(result)
    assert '둘째 후보' in section(html, 'incident-analysis')
    assert collected(html).count('support-source') == 1
    output = render_text(result).split('결론', 1)[1].split('핵심 요약', 1)[0]
    assert output.count('support-source') == 3
    assert output.index('반증') < output.index('둘째 후보')


def test_unreferenced_evidence_is_not_listed_as_narrative_citation():
    base = example()
    result = replace(base, narrative=replace(base.narrative, causes=(), recommendations=()))
    html = render_report(result)
    assert 'id="citations"' not in html
    assert 'support-source' not in render_text(result).split('결론', 1)[1].split('핵심 요약', 1)[0]


def test_action_only_common_and_finding_sources_are_not_lost():
    base = example()
    evidence = base.evidence[0]
    citations = tuple(EvidenceCitation(f'S{i}', evidence.model_copy(update={
        'evidence_id': f'S{i}', 'message': name,
    })) for i, name in enumerate(('action-source', 'common-source', 'finding-source')))
    narrative = replace(base.narrative,
        recommendations=(
            Recommendation('후보 확인', citations=(citations[0],), cause_index=0),
            Recommendation('공통 확인', citations=(citations[1],)),
        ),
        findings=(Finding('Info', '관측 사항', citations=(citations[2],), detail='관측 설명'),),
    )
    result = replace(base, narrative=narrative, evidence=(*base.evidence, *(c.evidence for c in citations)))
    text = render_text(result)
    text_citations = text.split('주요 관측', 1)[0].split('결론', 1)[1] + '주요 관측' + text.split('주요 관측', 1)[1].split('주요 타임라인', 1)[0]
    for output in (collected(render_report(result)), text_citations):
        assert output.count('action-source') == 1
        assert output.count('common-source') == 1
        assert output.count('finding-source') == 1


@pytest.mark.parametrize(('status', 'cause_shown'), [('MISMATCH', False), ('NOT_VERIFIED', True)])
def test_failed_verification_keeps_sections_and_collection_notices(status, cause_shown):
    base = example()
    failed = SourceWindowStatus('node_log', T0, T0 + timedelta(minutes=1), 'failed', None, T0, 'ssh refused')
    result = replace(base, verification_status=status, evidence=(),
                     observations=replace(base.observations, source_statuses=(*base.observations.source_statuses, failed)))
    html = render_report(result, gaps=('리포트 검증 불일치: internal reason',))
    assert ('가능성: 처리 대기' in section(html, 'incident-analysis')) is cause_shown
    assert 'internal reason' not in html
    assert 'SSH 수집 실패: ssh refused' in section(html, 'ssh-logs')
    assert '<td>12</td>' in section(html, 'query-trend')
    for name in ('overview', 'incident-analysis', 'query-trend', 'timeline', 'es-query-log', 'master-logs', 'slowlogs', 'ssh-logs'):
        assert f'id="{name}"' in html
