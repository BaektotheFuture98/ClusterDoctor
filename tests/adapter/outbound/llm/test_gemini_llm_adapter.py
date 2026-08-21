from datetime import datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest

from cluster_doctor.adapter.outbound.llm.gemini_llm_adapter import (
    GeminiApiError,
    GeminiLlmAdapter,
    GeminiResponseError,
    build_prompt,
)
from cluster_doctor.config.settings import Settings
from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange

TR = TimeRange(start=datetime(2026, 8, 20, 2, 9), end=datetime(2026, 8, 20, 2, 10))

# Anchored to the shipped default, so the URL assertions below exercise the
# real endpoint an unconfigured deployment would call -- not just the
# adapter's f-string shape.
BASE_URL = Settings.model_fields["gemini_base_url"].default
API_KEY  = "secret-key-abc123"

GEMINI_OK = {"candidates": [{"content": {"parts": [{"text": "분석 완료"}]}}]}

# gemini-2.5-* thinking tokens can exhaust maxOutputTokens: content is present
# but carries no "parts" key at all.
GEMINI_MAX_TOKENS = {
    "candidates": [{"content": {"role": "model"}, "finishReason": "MAX_TOKENS"}]
}

# A safety-blocked prompt comes back with no candidates whatsoever.
GEMINI_SAFETY_BLOCK = {"promptFeedback": {"blockReason": "SAFETY"}}


def _log(source: str, level: str, idx: int = 0) -> LogEntry:
    return LogEntry(
        timestamp=datetime(2026, 8, 20, 2, 9, 0, microsecond=idx),
        level=level, source=source, message=f"msg{idx}",
        node="node1", component="svc1",
    )


def _patched_client(response_json):
    mock_resp = MagicMock()
    mock_resp.json.return_value = response_json
    mock_resp.raise_for_status = MagicMock()
    mock_client = MagicMock()
    mock_client.post.return_value = mock_resp
    return mock_client


def _adapter() -> GeminiLlmAdapter:
    return GeminiLlmAdapter(
        api_key=API_KEY, base_url=BASE_URL, default_model="gemini-2.5-flash"
    )


def _call(adapter: GeminiLlmAdapter, response_json, model=None):
    """Run ``analyze`` against a stubbed httpx client, returning (result, client)."""
    mock_client = _patched_client(response_json)
    with patch("httpx.Client") as cls:
        cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        cls.return_value.__exit__  = MagicMock(return_value=False)
        result = adapter.analyze(TR, [], model)
    return result, mock_client


def test_analyze_uses_default_model_when_none():
    result, mock_client = _call(_adapter(), GEMINI_OK)
    assert result == "분석 완료"
    # Full URL, not a substring: the missing /v1beta segment survived a
    # substring-only assertion and 404'd every real call.
    assert mock_client.post.call_args[0][0] == (
        "https://generativelanguage.googleapis.com/v1beta"
        "/models/gemini-2.5-flash:generateContent"
    )


def test_analyze_uses_provided_model():
    _, mock_client = _call(_adapter(), GEMINI_OK, model="gemini-2.5-pro")
    assert mock_client.post.call_args[0][0] == (
        "https://generativelanguage.googleapis.com/v1beta"
        "/models/gemini-2.5-pro:generateContent"
    )


def test_analyze_sends_api_key_as_header_and_never_in_url():
    _, mock_client = _call(_adapter(), GEMINI_OK)
    url     = mock_client.post.call_args[0][0]
    headers = mock_client.post.call_args[1]["headers"]
    assert headers["x-goog-api-key"] == API_KEY
    assert API_KEY not in url
    assert "key=" not in url


def test_analyze_sends_correct_generation_config():
    _, mock_client = _call(_adapter(), GEMINI_OK)
    payload = mock_client.post.call_args[1]["json"]
    cfg     = payload["generationConfig"]
    assert cfg["temperature"]      == 0.2
    assert cfg["maxOutputTokens"]  == 8192


def test_analyze_raises_status_code_only_error_without_leaking_key():
    request  = httpx.Request("POST", f"{BASE_URL}/models/x:generateContent?key={API_KEY}")
    response = httpx.Response(429, request=request, text="quota exceeded for project 12345")

    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "boom", request=request, response=response
    )
    mock_client = MagicMock()
    mock_client.post.return_value = mock_resp

    with patch("httpx.Client") as cls:
        cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        cls.return_value.__exit__  = MagicMock(return_value=False)
        with pytest.raises(GeminiApiError) as excinfo:
            _adapter().analyze(TR, [], None)

    message = str(excinfo.value)
    assert "429" in message
    assert API_KEY not in message
    assert "quota exceeded" not in message
    # The URL-bearing httpx error must not be chained in, or the key lands in
    # the logged traceback anyway.
    assert excinfo.value.__cause__ is None
    assert excinfo.value.__suppress_context__ is True


def test_analyze_raises_clear_error_when_max_tokens_leaves_no_parts():
    with pytest.raises(GeminiResponseError) as excinfo:
        _call(_adapter(), GEMINI_MAX_TOKENS)
    assert "MAX_TOKENS" in str(excinfo.value)


def test_analyze_raises_clear_error_when_prompt_is_safety_blocked():
    with pytest.raises(GeminiResponseError) as excinfo:
        _call(_adapter(), GEMINI_SAFETY_BLOCK)
    assert "SAFETY" in str(excinfo.value)


def test_build_prompt_limits_to_200_per_source():
    logs   = [_log("slowlog", "SLOWLOG", i) for i in range(250)]
    prompt = build_prompt(TR, logs)
    lines  = [l for l in prompt.split("\n") if "msg" in l]
    assert len(lines) == 200


def test_build_prompt_discloses_true_total_and_sampled_count():
    logs   = [_log("slowlog", "SLOWLOG", i) for i in range(250)]
    prompt = build_prompt(TR, logs)
    header = next(l for l in prompt.split("\n") if l.startswith("[slowlog]"))
    # The model must not be told the sampled count as if it were the total:
    # its first analysis principle asks it to judge by frequency.
    assert "250" in header
    assert "200" in header
    assert header.endswith("총 250건 중 200건 샘플")


def test_build_prompt_header_matches_total_when_under_sample_cap():
    logs   = [_log("slowlog", "SLOWLOG", i) for i in range(3)]
    prompt = build_prompt(TR, logs)
    header = next(l for l in prompt.split("\n") if l.startswith("[slowlog]"))
    assert header.endswith("총 3건 중 3건 샘플")


def test_build_prompt_groups_by_source():
    logs   = [_log("slowlog", "SLOWLOG"), _log("es_query_log", "SUCCESS"), _log("node_metric", "METRIC")]
    prompt = build_prompt(TR, logs)
    assert "[slowlog]"      in prompt
    assert "[es_query_log]" in prompt
    assert "[node_metric]"  in prompt


def test_build_prompt_contains_analysis_principles():
    prompt = build_prompt(TR, [])
    assert "임계치 초과" in prompt
    assert "특이사항 없음" in prompt
    assert "FAIL" in prompt
    assert "토큰 분석 이슈" in prompt
    assert "광범위 범위 검색" in prompt


def test_build_prompt_contains_response_format():
    prompt = build_prompt(TR, [])
    assert "마크다운 금지" in prompt
    assert "클러스터 건강 상태" in prompt
    assert "요청 이해" in prompt
