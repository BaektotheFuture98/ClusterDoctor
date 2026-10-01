"""Actual-report interpretation and candidate attribution contracts."""
from dataclasses import replace
import pytest
from cluster_doctor.incident_orchestrator_agent.model.incident_report import Narrative, CauseAssessment, Recommendation, Finding
from cluster_doctor.incident_orchestrator_agent.model.evidence_citation import EvidenceCitation
from cluster_doctor.incident_analysis_agent.model.report import SuspectPick
from cluster_doctor.incident_analysis_agent.service.observation.compute import slow_candidates
from tests.unit.incident_orchestrator_agent.service.test_report_contract import report
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.html_file_notifier import render_report


def test_verified_cause_and_action_include_visible_original_source():
    r=report('PASSED'); e=r.evidence[0]; citation=EvidenceCitation(e.evidence_id,e)
    n=Narrative(causes=(CauseAssessment(statement='가설',confidence='Low',supporting=(citation,),contradicting=(citation,)),),
        recommendations=(Recommendation('확인 목적',citations=(citation,)),))
    html=render_report(replace(r,narrative=n))
    assert '가설' in html and '판단 근거' in html and '반증' in html and '확신도: Low' in html
    assert '확인 목적' in html and '/es/log' in html and 'WARN &lt;stack&gt;&amp;' in html
    assert e.evidence_id not in html and 'href="#evidence' not in html


@pytest.mark.parametrize('status',['NOT_VERIFIED','MISMATCH'])
def test_unverified_interpretations_and_actions_are_not_promoted(status):
    r=report(status)
    n=Narrative(headline='claim',causes=(CauseAssessment(statement='cause'),),recommendations=('restart now',))
    html=render_report(replace(r,narrative=n))
    assert 'claim' not in html and 'cause</h3>' not in html and 'restart now' not in html
    assert '1.96s' in html


def test_dangling_reference_has_no_id_or_link():
    r=report('PASSED')
    n=Narrative(causes=(CauseAssessment(statement='hypothesis',supporting=(EvidenceCitation('MISSING-ID',None),)),))
    html=render_report(replace(r,narrative=n))
    assert '인용 원문 확인 불가' in html and 'MISSING-ID' not in html


def test_finding_title_detail_are_escaped():
    r=report('PASSED')
    html=render_report(replace(r,narrative=Narrative(findings=(Finding(severity='Warning',title='<b>title</b>',detail='<script>detail</script>'),))))
    assert '&lt;b&gt;title&lt;/b&gt;' in html and '&lt;script&gt;detail&lt;/script&gt;' in html
    assert '<script>' not in html


def test_pick_reason_is_attached_only_to_unambiguous_actual_record():
    r=report('PASSED'); rows=r.observations.query_requests; c=replace(slow_candidates(list(rows),limit=1)[0],candidate_id='C1')
    r=replace(r,observations=replace(r.observations,candidates=(c,)),narrative=Narrative(suspect_picks=(SuspectPick(c.candidate_id,'선정 근거'),)))
    html=render_report(r)
    assert '선정 근거' in html and c.candidate_id not in html
    ambiguous=replace(r,observations=replace(r.observations,query_requests=rows+(rows[0],)))
    assert '선정 근거' not in render_report(ambiguous)
