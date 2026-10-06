"""Narrative ordering and citation preservation in the actual renderers."""
from dataclasses import replace

import pytest

from cluster_doctor.incident_orchestrator_agent.model.evidence_citation import EvidenceCitation
from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    CauseAssessment, Finding, Narrative, Recommendation,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import render_report
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import render_text
from tests.unit.incident_orchestrator_agent.service.test_report_contract import report


def section(html, name):
    return html.split(f'id="{name}"', 1)[1].split('</section>', 1)[0]


def example():
    base = report('PASSED')
    evidence = base.evidence[0].model_copy(update={
        'message': 'support-source', 'time_origin': 'parsed',
    })
    citation = EvidenceCitation(evidence.evidence_id, evidence)
    narrative = Narrative(
        headline='현상과 판단 한계를 연결한 요약', headline_citations=(citation,),
        causes=(CauseAssessment(
            statement='가능성: 처리 대기', confidence='Low',
            mechanism='관측을 설명하는 첫 문단.\n\n조건부 영향 경로 <tag>&.',
            uncertainties=('요청 대상 연결 미확인',), supporting=(citation,),
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
    summary = section(html, 'summary')
    causes = section(html, 'causes')
    assert '현상과 판단 한계를 연결한 요약' in summary
    assert '<pre' not in summary
    assert causes.index('관측을 설명하는 첫 문단.') < causes.index('요청 대상을 먼저 식별한다.') < causes.index('support-source')
    assert causes.count('support-source') == 1
    assert '<p>조건부 영향 경로 &lt;tag&gt;&amp;.</p>' in causes
    text = render_text(result)
    summary_text = text.split('핵심 요약', 1)[1].split('쿼리 실행 추이', 1)[0]
    cause_text = text.split('원인 판단·조치', 1)[1]
    assert 'support-source' not in summary_text
    assert cause_text.index('관측을 설명하는 첫 문단.') < cause_text.index('요청 대상을 먼저 식별한다.') < cause_text.index('support-source')
    assert cause_text.count('support-source') == 1
    assert '첫 문단.\n\n조건부 영향 경로 <tag>&.' in cause_text


def test_same_source_is_preserved_for_distinct_causes_and_counter_evidence():
    base = example()
    citation = base.narrative.causes[0].supporting[0]
    first = replace(base.narrative.causes[0], contradicting=(citation,))
    second = CauseAssessment(statement='둘째 후보', mechanism='둘째 설명', supporting=(citation,))
    result = replace(base, narrative=replace(base.narrative, causes=(first, second)))
    for output in (section(render_report(result), 'causes'), render_text(result).split('원인 판단·조치', 1)[1]):
        assert output.count('support-source') == 3
        assert output.index('반증') < output.index('둘째 후보')


def test_summary_only_sources_survive_without_causes():
    base = example()
    result = replace(base, narrative=replace(base.narrative, causes=(), recommendations=()))
    html = render_report(result)
    assert 'support-source' not in section(html, 'summary')
    assert '종합 요약 근거' in section(html, 'causes')
    assert section(html, 'causes').count('support-source') == 1
    text = render_text(result)
    assert text.index('원인 판단·조치') < text.index('종합 요약 근거') < text.rindex('support-source')


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
    for output in (section(render_report(result), 'causes'), render_text(result).split('원인 판단·조치', 1)[1]):
        assert output.count('action-source') == 1
        assert output.count('common-source') == 1
        assert output.count('finding-source') == 1


@pytest.mark.parametrize('status', ['MISMATCH', 'NOT_VERIFIED'])
def test_failed_verification_keeps_sections_and_collection_notices(status):
    result = replace(example(), verification_status=status)
    html = render_report(result, gaps=('리포트 검증 불일치: internal reason', 'SSH 수집 실패'))
    assert '현상과 판단 한계를 연결한 요약' not in section(html, 'summary')
    assert 'internal reason' not in html
    assert 'SSH 수집 실패' in html
    assert '12건' in section(html, 'summary')
    for name in ('query-trend', 'timeline', 'query-ranking', 'master-logs', 'slowlogs', 'ssh-logs', 'causes'):
        assert f'id="{name}"' in html
