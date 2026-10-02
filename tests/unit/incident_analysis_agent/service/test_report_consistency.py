from datetime import UTC, datetime, timedelta
from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport, ReportFinding, RootCause, TimelineEvent
from cluster_doctor.incident_analysis_agent.service.validation.consistency.report_validation import validate_report

T0=datetime(2026,10,1,tzinfo=UTC)


def test_unrelated_findings_do_not_set_global_cause_deadline():
    early=Evidence(evidence_id='early',event_time=T0,source=EvidenceSource.QUERY_LOG,message='early')
    late=Evidence(evidence_id='late',event_time=T0+timedelta(minutes=3),source=EvidenceSource.NODE_LOG,message='later')
    report=LogAnalysisReport(incident_id='i',analyzed_from=T0,analyzed_to=T0+timedelta(minutes=5),
        findings=(ReportFinding(title='서로 다른 초기 현상',evidence_refs=('early',)),),
        root_causes=(RootCause(statement='후반 현상에 대한 후보',confidence='Low',supporting_evidence_refs=('late',)),))
    assert not any('보다 늦다' in x for x in validate_report(report,[early,late]).issues)


def test_fallback_time_is_left_to_prompt_validation():
    e=Evidence(evidence_id='unknown',event_time=T0,source=EvidenceSource.NODE_LOG,message='undated',time_origin='fallback')
    report=LogAnalysisReport(incident_id='i',analyzed_from=T0,analyzed_to=T0,
        timeline=(TimelineEvent(at=T0,description='그 시각에 발생',evidence_refs=('unknown',)),))
    assert not validate_report(report,[e]).issues


def test_rounded_time_is_left_to_prompt_validation():
    e=Evidence(evidence_id='exact',event_time=T0+timedelta(seconds=7),source=EvidenceSource.NODE_LOG,message='timed')
    report=LogAnalysisReport(incident_id='i',analyzed_from=T0,analyzed_to=T0+timedelta(minutes=1),
        timeline=(TimelineEvent(at=T0,description='잘못된 시각',evidence_refs=('exact',)),))
    assert not validate_report(report,[e]).issues


def test_confidence_and_causal_language_are_left_to_prompt_validation():
    report=LogAnalysisReport(incident_id='i',analyzed_from=T0,analyzed_to=T0,
        root_causes=(RootCause(statement='원인이다',confidence='High'),))
    assert not validate_report(report,[]).issues
