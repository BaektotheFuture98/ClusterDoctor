import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

from cluster_doctor.exceptions import LlmApiError
from cluster_doctor.incident_analysis_agent.model.basemodel.evidence import (
    Evidence,
    EvidenceSource,
)
from cluster_doctor.incident_analysis_agent.model.basemodel.report import (
    LogAnalysisReport,
    TimelineEvent,
)
from cluster_doctor.incident_analysis_agent.model.basemodel.validation_types import (
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
    validator, _ = _make_validator(json.dumps([]))
    assert validator.validate(_make_report(), [_evidence()]) == []


def test_grounding_mismatch_returns_issue():
    mismatch = json.dumps(
        [
            {
                "claim": "cpu spike detected",
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
    body = json.dumps([{"status": "MISMATCH", "reason": "x", "kind": "report_mismatch"}])
    validator, _ = _make_validator("```json\n" + body + "\n```")
    issues = validator.validate(_make_report(), [_evidence()])
    assert issues[0].issue_type == VerificationIssueType.REPORT_MISMATCH


def test_llm_is_called_with_messages_and_max_tokens():
    validator, call_llm = _make_validator("[]")
    validator.validate(_make_report(), [_evidence()])
    messages, max_tokens = call_llm.call_args.args
    assert messages[0]["role"] == "user"
    assert "cpu_usage=10%" in messages[0]["content"]
    assert isinstance(max_tokens, int)


def test_no_claims_skips_llm():
    validator, call_llm = _make_validator("[]")
    empty = LogAnalysisReport(incident_id="INC-1", analyzed_from=_T0, analyzed_to=_T0)
    assert validator.validate(empty, [_evidence()]) == []
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


def test_missing_raw_shows_placeholder_in_prompt():
    call_llm = MagicMock(return_value="[]")
    validator = GroundingValidator(call_llm=call_llm)

    validator.validate(_make_report(), [_evidence(raw=None)])

    messages, _ = call_llm.call_args.args
    assert "(raw 없음)" in messages[0]["content"]


def test_raw_text_is_truncated_to_max_chars():
    call_llm = MagicMock(return_value="[]")
    validator = GroundingValidator(call_llm=call_llm)

    validator.validate(_make_report(), [_evidence(raw="x" * 3000)])

    messages, _ = call_llm.call_args.args
    content = messages[0]["content"]
    assert "x" * 2000 in content
    assert "x" * 2001 not in content
