from datetime import UTC, datetime, timedelta
from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.service.report_generation.schema import parse_draft
from cluster_doctor.incident_analysis_agent.service.validation.consistency.report_validation import validate_report
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.output_mapping import to_incident_analysis_report

T0=datetime(2026,10,1,tzinfo=UTC)
E=Evidence(evidence_id='E-one',event_time=T0,source=EvidenceSource.QUERY_LOG,message='query')


def domain(payload):
    return parse_draft(payload).to_domain(incident_id='i',window=TimeRange(T0,T0+timedelta(minutes=1)),evidence_refs=('E-one',))


def test_summary_refs_survive_projection():
    report=domain('{"summary":"요약","summary_evidence_refs":["E-one"]}')
    result=to_incident_analysis_report(report,Observations(),[E])
    assert result.narrative.headline_citations[0].evidence == E
    assert report.cited_refs()=={'E-one'}


def test_recommendation_refs_survive_projection():
    report=domain('{"recommendations":[{"text":"대상 쿼리 확인","evidence_refs":["E-one"]}]}')
    result=to_incident_analysis_report(report,Observations(),[E])
    action=result.narrative.recommendations[0]
    assert action.text=='대상 쿼리 확인'
    assert action.citations[0].evidence==E
    assert report.cited_refs()=={'E-one'}


def test_legacy_string_recommendation_is_sent_to_model_without_ref_requirement():
    report=domain('{"recommendations":["기존 권고"]}')
    assert report.recommendations[0].text=='기존 권고'
    assert report.recommendations[0].evidence_refs==()
    assert not validate_report(report,[E]).issues


def test_unknown_summary_ref_is_preserved_for_validation():
    report=domain('{"summary":"요약","summary_evidence_refs":["E-missing"]}')
    assert 'E-missing' in report.cited_refs()
    assert any('E-missing' in x for x in validate_report(report,[E]).issues)
