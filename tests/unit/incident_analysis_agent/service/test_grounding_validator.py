import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

from cluster_doctor.exceptions import LlmApiError
from cluster_doctor.incident_analysis_agent.model.evidence import (
    Evidence,
    EvidenceSource,
)
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisReport,
    TimelineEvent,
)
from cluster_doctor.incident_analysis_agent.model.validation import (
    VerificationIssueType,
)
from cluster_doctor.incident_analysis_agent.service.validation.grounding.grounding_validator import (
    GroundingValidator,
)

_T0 = datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc)


def _evidence(raw: str | None = "cpu_usage=10%") -> Evidence:
    return Evidence(
        evidence_id="E-INC-1-1",
        event_time=_T0,
        source=EvidenceSource.NODE_METRIC,
        message="cpu normal",
        raw=raw,
    )


def _make_validator(llm_response: str):
    call_llm = MagicMock(return_value=llm_response)
    return GroundingValidator(call_llm=call_llm), call_llm


def _make_report(description: str = "cpu normal") -> LogAnalysisReport:
    return LogAnalysisReport(
        incident_id="INC-1",
        analyzed_from=_T0,
        analyzed_to=_T0,
        timeline=(
            TimelineEvent(at=_T0, description=description, evidence_refs=("E-INC-1-1",)),
        ),
        evidence_refs=("E-INC-1-1",),
    )


def test_grounding_passed_returns_empty():
    validator, _ = _make_validator(json.dumps([{"claim_id":"timeline:0","status":"PASSED"}]))
    assert validator.validate(_make_report(), [_evidence()]) == []


def test_grounding_mismatch_returns_issue():
    mismatch = json.dumps(
        [
            {
                "claim_id": "timeline:0",
                "status": "MISMATCH",
                "reason": "raw shows cpu=10%, no spike",
                "kind": "analysis_mismatch",
                "affected_evidence_refs": ["E-INC-1-1"],
            }
        ]
    )
    validator, _ = _make_validator(mismatch)
    issues = validator.validate(_make_report("cpu spike detected"), [_evidence()])
    assert len(issues) == 1
    assert issues[0].issue_type == VerificationIssueType.ANALYSIS_MISMATCH
    assert issues[0].evidence_refs == ("E-INC-1-1",)


def test_unknown_kind_becomes_unverifiable():
    payload = json.dumps([{"status": "MISMATCH", "reason": "?", "kind": "weird"}])
    validator, _ = _make_validator(payload)
    issues = validator.validate(_make_report(), [_evidence()])
    assert issues[0].issue_type == VerificationIssueType.UNVERIFIABLE


def test_code_fenced_json_is_parsed():
    body = json.dumps([{"claim_id":"timeline:0", "status": "MISMATCH", "reason": "x", "kind": "report_mismatch"}])
    validator, _ = _make_validator("```json\n" + body + "\n```")
    issues = validator.validate(_make_report(), [_evidence()])
    assert issues[0].issue_type == VerificationIssueType.REPORT_MISMATCH


def test_llm_is_called_with_messages_only():
    validator, call_llm = _make_validator("[]")
    validator.validate(_make_report(), [_evidence()])
    (messages,) = call_llm.call_args.args
    assert messages[0]["role"] == "user"
    assert "cpu_usage=10%" in messages[0]["content"]


def test_no_claims_skips_llm():
    validator, call_llm = _make_validator("[]")
    empty = LogAnalysisReport(incident_id="INC-1", analyzed_from=_T0, analyzed_to=_T0)
    assert validator.validate(empty, [_evidence()])[0].issue_type == VerificationIssueType.UNVERIFIABLE
    call_llm.assert_not_called()


def test_unparseable_response_is_unverifiable_not_passed():
    validator, _ = _make_validator("not json")
    issues = validator.validate(_make_report(), [_evidence()])
    assert [i.issue_type for i in issues] == [VerificationIssueType.UNVERIFIABLE]


def test_llm_failure_is_unverifiable_not_passed():
    validator, call_llm = _make_validator("[]")
    call_llm.side_effect = LlmApiError("boom")
    issues = validator.validate(_make_report(), [_evidence()])
    assert [i.issue_type for i in issues] == [VerificationIssueType.UNVERIFIABLE]


def test_missing_raw_is_unverifiable_without_model_call():
    validator, call = _make_validator('[]')
    assert validator.validate(_make_report(), [_evidence(raw=None)])[0].issue_type == VerificationIssueType.UNVERIFIABLE
    call.assert_not_called()


def test_raw_beyond_2000_is_preserved_and_truncated_raw_cannot_pass():
    validator, call = _make_validator('[{"claim_id":"timeline:0","status":"PASSED"}]')
    assert validator.validate(_make_report(), [_evidence(raw='x'*3000)]) == []
    assert 'x'*3000 in call.call_args.args[0][0]['content']
    assert validator.validate(_make_report(), [_evidence().model_copy(update={'raw_truncated':True})])[0].issue_type == VerificationIssueType.UNVERIFIABLE


def test_empty_verdict_is_unverifiable():
    validator, _ = _make_validator('[]')
    assert validator.validate(_make_report(), [_evidence()])[0].issue_type == VerificationIssueType.UNVERIFIABLE


def test_summary_only_is_checked_with_stable_claim_id():
    report = _make_report().model_copy(update={'timeline': (), 'summary': 'cpu spike', 'summary_evidence_refs': ('E-INC-1-1',)})
    validator, call = _make_validator(json.dumps([{'claim_id':'summary','status':'PASSED'}]))
    assert validator.validate(report, [_evidence()]) == []
    assert '"claim_id": "summary"' in call.call_args.args[0][0]['content']


def test_partial_duplicate_unknown_verdicts_fail_closed():
    import pytest
    for payload in ([{}], [{'claim_id':'timeline:0','status':'UNKNOWN'}],
        [{'claim_id':'unknown','status':'PASSED'}], [{'claim_id':'timeline:0','status':'PASSED'}]*2):
        validator, _ = _make_validator(json.dumps(payload))
        assert validator.validate(_make_report(), [_evidence()])[0].issue_type == VerificationIssueType.UNVERIFIABLE


def test_all_fields_and_counter_evidence_are_in_claim_contract():
    from cluster_doctor.incident_analysis_agent.model.report import ReportFinding, RootCause, ReportRecommendation, SuspectPick
    report = _make_report().model_copy(update={
        'summary':'summary', 'summary_evidence_refs':('E-INC-1-1',),
        'findings':(ReportFinding(title='wrong title', detail='correct detail', evidence_refs=('E-INC-1-1',)),),
        'root_causes':(RootCause(statement='other node GC caused delay', supporting_evidence_refs=('E-INC-1-1',), counter_evidence_refs=('counter',)),),
        'recommendations':(ReportRecommendation(text='restart', evidence_refs=('E-INC-1-1',)),),
        'suspect_picks':(SuspectPick(candidate_id='C1', reason='slow'),)})
    claims = GroundingValidator._extract_claims(report)
    assert {c['claim_id'] for c in claims} == {'summary','timeline:0','finding:0:title','finding:0:detail','cause:0','recommendation:0','suspect:0'}
    assert next(c for c in claims if c['claim_id']=='cause:0')['counter_evidence_refs'] == ['counter']
    validator, _ = _make_validator('[{"claim_id":"timeline:0","status":"PASSED"}]')
    assert any(i.issue_type == VerificationIssueType.UNVERIFIABLE for i in validator.validate(report, [_evidence()]))


def test_cross_claim_verdict_reference_is_unverifiable():
    issues=GroundingValidator._parse_response(json.dumps([
        {'claim_id':'a','status':'PASSED','affected_evidence_refs':['B']},
        {'claim_id':'b','status':'PASSED','affected_evidence_refs':['B']}]),
        expected_claim_ids={'a','b'},known_evidence_refs={'A','B'},claim_evidence_refs={'a':{'A'},'b':{'B'}})
    assert any(i.issue_type == VerificationIssueType.UNVERIFIABLE for i in issues)
