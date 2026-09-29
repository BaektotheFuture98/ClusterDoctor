import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

from cluster_doctor.adapters.outbound.deepagents.analysis.pipeline.grounding_validator import (
    GroundingValidator,
)
from cluster_doctor.domain.analysis.report import LogAnalysisReport, TimelineEvent
from cluster_doctor.domain.analysis.validation_types import VerificationIssueType
from cluster_doctor.exceptions import LlmApiError

_T0 = datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc)


def _make_validator(llm_response: str):
    store = MagicMock()
    store.get_evidence.return_value = [
        MagicMock(evidence_id="E-INC-1-1", raw_ref="R-INC-1-1", message="cpu normal")
    ]
    store.get_raw.return_value = "cpu_usage=10%"
    call_llm = MagicMock(return_value=llm_response)
    return GroundingValidator(store=store, call_llm=call_llm), call_llm


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
    assert validator.validate(_make_report(), "INC-1") == []


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
    issues = validator.validate(_make_report("cpu spike detected"), "INC-1")
    assert len(issues) == 1
    assert issues[0].issue_type == VerificationIssueType.ANALYSIS_MISMATCH
    assert issues[0].evidence_refs == ("E-INC-1-1",)


def test_unknown_kind_becomes_unverifiable():
    payload = json.dumps([{"status": "MISMATCH", "reason": "?", "kind": "weird"}])
    validator, _ = _make_validator(payload)
    issues = validator.validate(_make_report(), "INC-1")
    assert issues[0].issue_type == VerificationIssueType.UNVERIFIABLE


def test_code_fenced_json_is_parsed():
    body = json.dumps([{"status": "MISMATCH", "reason": "x", "kind": "report_mismatch"}])
    validator, _ = _make_validator("```json\n" + body + "\n```")
    issues = validator.validate(_make_report(), "INC-1")
    assert issues[0].issue_type == VerificationIssueType.REPORT_MISMATCH


def test_llm_is_called_with_messages_and_max_tokens():
    validator, call_llm = _make_validator("[]")
    validator.validate(_make_report(), "INC-1")
    messages, max_tokens = call_llm.call_args.args
    assert messages[0]["role"] == "user"
    assert "cpu_usage=10%" in messages[0]["content"]
    assert isinstance(max_tokens, int)


def test_no_claims_skips_llm():
    validator, call_llm = _make_validator("[]")
    empty = LogAnalysisReport(incident_id="INC-1", analyzed_from=_T0, analyzed_to=_T0)
    assert validator.validate(empty, "INC-1") == []
    call_llm.assert_not_called()


def test_unparseable_response_is_unverifiable_not_passed():
    validator, _ = _make_validator("not json")
    issues = validator.validate(_make_report(), "INC-1")
    assert [i.issue_type for i in issues] == [VerificationIssueType.UNVERIFIABLE]


def test_llm_failure_is_unverifiable_not_passed():
    validator, call_llm = _make_validator("[]")
    call_llm.side_effect = LlmApiError("boom")
    issues = validator.validate(_make_report(), "INC-1")
    assert [i.issue_type for i in issues] == [VerificationIssueType.UNVERIFIABLE]


def test_missing_raw_ref_shows_placeholder_in_prompt():
    store = MagicMock()
    store.get_evidence.return_value = [
        MagicMock(evidence_id="E-INC-1-1", raw_ref=None)
    ]
    call_llm = MagicMock(return_value="[]")
    validator = GroundingValidator(store=store, call_llm=call_llm)

    validator.validate(_make_report(), "INC-1")

    messages, _ = call_llm.call_args.args
    assert "(raw 없음)" in messages[0]["content"]
    store.get_raw.assert_not_called()


def test_raw_text_is_truncated_to_max_chars():
    store = MagicMock()
    store.get_evidence.return_value = [
        MagicMock(evidence_id="E-INC-1-1", raw_ref="R-INC-1-1")
    ]
    store.get_raw.return_value = "x" * 3000
    call_llm = MagicMock(return_value="[]")
    validator = GroundingValidator(store=store, call_llm=call_llm)

    validator.validate(_make_report(), "INC-1")

    messages, _ = call_llm.call_args.args
    content = messages[0]["content"]
    assert "x" * 2000 in content
    assert "x" * 2001 not in content
