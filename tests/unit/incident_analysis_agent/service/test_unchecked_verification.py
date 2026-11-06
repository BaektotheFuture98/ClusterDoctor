"""근거 대조를 한 건도 못 한 사건 종합은 UNCHECKED로 끝난다."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from cluster_doctor.exceptions import LlmApiError
from cluster_doctor.incident_analysis_agent.datasource.clickhouse.node_metric import DEFAULT_THRESHOLDS
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport, VerificationStatus
from cluster_doctor.incident_analysis_agent.model.validation import UNVERIFIABLE_PREFIX
from cluster_doctor.incident_analysis_agent.service.report_generation.incident_synthesis import synthesize_incident
from cluster_doctor.incident_analysis_agent.service.report_generation.report_writer import ReportWriter
from cluster_doctor.incident_analysis_agent.service.validation.grounding.grounding_validator import GroundingValidator

START = datetime(2026, 11, 6, 8, 0, tzinfo=timezone.utc)
_DRAFT = {'title': '제목', 'summary': '사건 요약', 'findings': [], 'unresolved_questions': [],
          'root_causes': [{'statement': '후보', 'mechanism': '처리 대기 가능성'}],
          'recommendations': [{'text': '대상 확인', 'cause_index': 0}]}


def _synthesize(grounding):
    def call(messages, **kwargs):
        if kwargs.get('response_format'):
            return json.dumps(_DRAFT, ensure_ascii=False)
        return grounding(messages)
    first = LogAnalysisReport(incident_id='I', analyzed_from=START, analyzed_to=START + timedelta(minutes=1))
    seams = SimpleNamespace(report_writer=ReportWriter(call_llm=call), wiki_investigator=None, call_llm=call,
                            metric_thresholds=DEFAULT_THRESHOLDS)
    return synthesize_incident(seams=seams, incident=SimpleNamespace(incident_id='I', cluster='C'),
                               window_reports=[first], observations=Observations(), evidence=[])


def _claims(messages):
    return json.loads(messages[0]['content'].split('Claims:\n')[1].split('\nEvidence:\n')[0])


def test_모든_대조_호출이_실패하면_UNCHECKED():
    def grounding(messages):
        raise LlmApiError('503')
    result = _synthesize(grounding)
    assert result.verification_status is VerificationStatus.UNCHECKED
    assert result.verification_issues
    assert all(i.startswith(UNVERIFIABLE_PREFIX) for i in result.verification_issues)


def test_일부_주장만_판정이_빠지면_NOT_VERIFIED():
    def grounding(messages):
        claims = _claims(messages)
        return json.dumps([{'claim_id': c['claim_id'], 'status': 'PASSED', 'affected_evidence_refs': []}
                           for c in claims[:-1]])
    assert _synthesize(grounding).verification_status is VerificationStatus.NOT_VERIFIED


def test_대조가_끝나면_PASSED():
    def grounding(messages):
        return json.dumps([{'claim_id': c['claim_id'], 'status': 'PASSED', 'affected_evidence_refs': []}
                           for c in _claims(messages)])
    assert _synthesize(grounding).verification_status is VerificationStatus.PASSED


def test_validate_with_coverage는_응답받은_배치의_주장_수를_센다():
    report = LogAnalysisReport(incident_id='I', analyzed_from=START, analyzed_to=START, summary='요약')
    ok = GroundingValidator(call_llm=lambda m, **k: json.dumps(
        [{'claim_id': 'summary', 'status': 'PASSED', 'affected_evidence_refs': []}]))
    issues, covered = ok.validate_with_coverage(report, [])
    assert (issues, covered) == ([], 1)

    def fail(messages, **kwargs):
        raise LlmApiError('503')
    issues, covered = GroundingValidator(call_llm=fail).validate_with_coverage(report, [])
    assert covered == 0 and issues[0].reason.startswith(UNVERIFIABLE_PREFIX)


def test_주장이_없으면_coverage_0이어도_기존_이슈를_그대로_낸다():
    report = LogAnalysisReport(incident_id='I', analyzed_from=START, analyzed_to=START)
    issues, covered = GroundingValidator(call_llm=lambda m, **k: '[]').validate_with_coverage(report, [])
    assert covered == 0 and issues[0].reason.endswith('검증할 분석 주장이 없다.')
