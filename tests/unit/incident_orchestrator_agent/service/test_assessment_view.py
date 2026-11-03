from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.report_layout import PRINT_OPEN_SCRIPT
"""Actual-report interpretation and candidate attribution contracts."""
from dataclasses import replace
from cluster_doctor.incident_orchestrator_agent.model.incident_report import Narrative, CauseAssessment, Recommendation, Finding
from cluster_doctor.incident_orchestrator_agent.model.evidence_citation import EvidenceCitation
from cluster_doctor.incident_analysis_agent.model.report import SuspectPick
from cluster_doctor.incident_analysis_agent.service.observation.compute import slow_candidates
from tests.unit.incident_orchestrator_agent.service.test_report_contract import report
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.report_renderer import render_report
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import render_text


def test_verified_cause_and_action_include_visible_original_source():
    r=report('PASSED'); e=r.evidence[0]; citation=EvidenceCitation(e.evidence_id,e)
    n=Narrative(causes=(CauseAssessment(statement='가설',supporting=(citation,),contradicting=(citation,)),),
        recommendations=(Recommendation('확인 목적',citations=(citation,)),))
    html=render_report(replace(r,narrative=n))
    assert '가설' in html and '확인 목적' in html
    collected=html[html.index('<details id="evidence">'):]
    assert '/es/log' in collected and 'WARN &lt;stack&gt;&amp;' in collected
    assert e.evidence_id not in html and 'href="#evidence' not in html


def test_mismatched_interpretations_and_actions_are_not_promoted():
    r=report('MISMATCH')
    n=Narrative(causes=(CauseAssessment(statement='cause'),),recommendations=('restart now',))
    html=render_report(replace(r,narrative=n))
    assert 'cause</h4>' not in html and 'restart now' not in html
    assert '1.96s' in html


def test_partially_verified_interpretations_and_actions_are_shown_with_notice():
    r=report('NOT_VERIFIED')
    n=Narrative(causes=(CauseAssessment(statement='cause'),),recommendations=('restart now',))
    html=render_report(replace(r,narrative=n))
    assert 'cause</h4>' in html and 'restart now' in html and '일부 검증' in html
    assert '1.96s' in html


def test_dangling_reference_has_no_id_or_link():
    r=report('PASSED')
    n=Narrative(causes=(CauseAssessment(statement='hypothesis',supporting=(EvidenceCitation('MISSING-ID',None),)),))
    assert 'MISSING-ID' not in render_report(replace(r,narrative=n))
    text=render_text(replace(r,narrative=n))
    assert '인용 원문 확인 불가' in text and 'MISSING-ID' not in text


def test_narrative_text_is_escaped_and_finding_markup_is_not_emitted():
    r=report('PASSED')
    n=Narrative(causes=(CauseAssessment(statement='<b>cause</b>',mechanism='<i>mechanism</i>'),),recommendations=(Recommendation('<u>action</u>'),),
        findings=(Finding(severity='Warning',title='<b>title</b>',detail='<script>detail</script>'),))
    html=render_report(replace(r,narrative=n))
    for escaped in ('&lt;b&gt;cause&lt;/b&gt;','&lt;i&gt;mechanism&lt;/i&gt;','&lt;u&gt;action&lt;/u&gt;'):
        assert escaped in html
    for raw in ('<b>','<i>mechanism','<u>action','<script>detail'):
        assert raw not in html
    assert '<script>' not in html.replace(PRINT_OPEN_SCRIPT,'')


def test_pick_reason_is_attached_only_to_unambiguous_actual_record():
    r=report('PASSED'); rows=r.observations.query_requests; c=replace(slow_candidates(list(rows),limit=1)[0],candidate_id='C1')
    r=replace(r,observations=replace(r.observations,candidates=(c,)),narrative=Narrative(suspect_picks=(SuspectPick(c.candidate_id,'선정 근거'),)))
    html=render_report(r)
    assert '선정 근거' in html and c.candidate_id not in html
    ambiguous=replace(r,observations=replace(r.observations,query_requests=rows+(rows[0],)))
    assert '선정 근거' not in render_report(ambiguous)
