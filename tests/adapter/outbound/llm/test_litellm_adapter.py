import logging
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from cluster_doctor.adapter.outbound.llm.litellm_adapter import LiteLlmAdapter
from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.domain.port.outbound.llm_analyzer import (
    LlmApiError,
    LlmResponseError,
)

TR = TimeRange(start=datetime(2026, 8, 20, 2, 9), end=datetime(2026, 8, 20, 2, 10))
CANARY_KEY = "sk-CANARY-DO-NOT-LOG-12345"


def _response(content, finish_reason="stop"):
    choice = MagicMock()
    choice.message.content = content
    choice.finish_reason = finish_reason
    resp = MagicMock()
    resp.choices = [choice]
    return resp


def _adapter(provider="gemini", model="gemini-2.5-flash"):
    return LiteLlmAdapter(provider=provider, api_key=CANARY_KEY, default_model=model)


def test_returns_the_completion_text():
    with patch("litellm.completion", return_value=_response("분석 완료")) as call:
        assert _adapter().analyze(TR, [], None) == "분석 완료"
    assert call.call_count == 1


def test_gemini_model_string_is_prefixed():
    with patch("litellm.completion", return_value=_response("ok")) as call:
        _adapter(provider="gemini", model="gemini-2.5-flash").analyze(TR, [], None)
    assert call.call_args.kwargs["model"] == "gemini/gemini-2.5-flash"


def test_nvidia_model_string_is_prefixed():
    with patch("litellm.completion", return_value=_response("ok")) as call:
        _adapter(provider="nvidia", model="meta/llama-3.3-70b-instruct").analyze(
            TR, [], None
        )
    assert call.call_args.kwargs["model"] == "nvidia_nim/meta/llama-3.3-70b-instruct"


def test_caller_supplied_model_overrides_the_default():
    with patch("litellm.completion", return_value=_response("ok")) as call:
        _adapter().analyze(TR, [], "gemini-2.5-pro")
    assert call.call_args.kwargs["model"] == "gemini/gemini-2.5-pro"


def test_generation_parameters_match_the_spec():
    with patch("litellm.completion", return_value=_response("ok")) as call:
        _adapter().analyze(TR, [], None)
    kwargs = call.call_args.kwargs
    assert kwargs["temperature"] == 0.2
    assert kwargs["max_tokens"] == 8192


def test_api_key_is_passed_as_a_parameter_not_embedded_in_the_model_string():
    with patch("litellm.completion", return_value=_response("ok")) as call:
        _adapter().analyze(TR, [], None)
    kwargs = call.call_args.kwargs
    assert kwargs["api_key"] == CANARY_KEY
    assert CANARY_KEY not in kwargs["model"]


def test_prompt_is_sent_as_a_single_user_message():
    logs = [
        LogEntry(
            timestamp=datetime(2026, 8, 20, 2, 9, 30),
            level="SLOWLOG",
            source="slowlog",
            message="probe-marker",
        )
    ]
    with patch("litellm.completion", return_value=_response("ok")) as call:
        _adapter().analyze(TR, logs, None)
    messages = call.call_args.kwargs["messages"]
    assert len(messages) == 1
    assert messages[0]["role"] == "user"
    assert "probe-marker" in messages[0]["content"]
    assert "총 1건 중 1건 샘플" in messages[0]["content"]


def test_unknown_provider_is_rejected_at_construction():
    with pytest.raises(ValueError, match="provider"):
        LiteLlmAdapter(provider="openai", api_key="k", default_model="gpt-4")


def test_truncated_response_raises_with_actionable_guidance():
    with patch("litellm.completion", return_value=_response(None, "length")):
        with pytest.raises(LlmResponseError) as excinfo:
            _adapter().analyze(TR, [], None)
    message = str(excinfo.value)
    assert "토큰 한도" in message
    assert "시간 범위" in message


def test_content_filter_response_lists_the_possible_causes():
    """원본 사유(SPII/BLOCKLIST/RECITATION 등)는 litellm이 content_filter로
    뭉치므로 복구할 수 없다. 대신 가능한 원인을 모두 열거한다."""
    with patch("litellm.completion", return_value=_response(None, "content_filter")):
        with pytest.raises(LlmResponseError) as excinfo:
            _adapter().analyze(TR, [], None)
    message = str(excinfo.value)
    assert "개인정보" in message
    assert "금지" in message
    assert "인용" in message


def test_empty_string_content_is_treated_as_no_text():
    with patch("litellm.completion", return_value=_response("", "stop")):
        with pytest.raises(LlmResponseError):
            _adapter().analyze(TR, [], None)


def test_provider_failure_reports_only_the_status_code():
    import litellm

    error = litellm.exceptions.RateLimitError(
        message=f"rate limited for url https://x/?key={CANARY_KEY}",
        llm_provider="gemini",
        model="gemini-2.5-flash",
    )
    with patch("litellm.completion", side_effect=error):
        with pytest.raises(LlmApiError) as excinfo:
            _adapter().analyze(TR, [], None)
    message = str(excinfo.value)
    assert CANARY_KEY not in message
    assert "429" in message


def test_api_key_never_reaches_the_log_records(caplog):
    """litellm이 자체 redaction을 걸어두지만, 그 보호에 의존하고 있다는 사실을
    우리 테스트로 고정한다. litellm이 동작을 바꾸면 여기서 잡힌다."""
    import litellm

    error = litellm.exceptions.AuthenticationError(
        message=f"invalid key {CANARY_KEY} at https://x/?key={CANARY_KEY}",
        llm_provider="gemini",
        model="gemini-2.5-flash",
    )
    with caplog.at_level(logging.DEBUG):
        with patch("litellm.completion", side_effect=error):
            with pytest.raises(LlmApiError):
                _adapter().analyze(TR, [], None)
    assert CANARY_KEY not in caplog.text
