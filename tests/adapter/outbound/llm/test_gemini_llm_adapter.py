from datetime import datetime
from unittest.mock import MagicMock, patch

from cluster_doctor.adapter.outbound.llm.gemini_llm_adapter import (
    GeminiLlmAdapter,
    _build_prompt,
)
from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange

TR = TimeRange(start=datetime(2026, 8, 20, 2, 9), end=datetime(2026, 8, 20, 2, 10))

GEMINI_OK = {"candidates": [{"content": {"parts": [{"text": "분석 완료"}]}}]}


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


def test_analyze_uses_default_model_when_none():
    adapter     = GeminiLlmAdapter(api_key="k", base_url="https://api.example.com", default_model="gemini-2.5-flash")
    mock_client = _patched_client(GEMINI_OK)
    with patch("httpx.Client") as cls:
        cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        cls.return_value.__exit__  = MagicMock(return_value=False)
        result = adapter.analyze(TR, [], None)
    assert result == "분석 완료"
    url = mock_client.post.call_args[0][0]
    assert "gemini-2.5-flash" in url
    assert "key=k" in url


def test_analyze_uses_provided_model():
    adapter     = GeminiLlmAdapter(api_key="k", base_url="https://api.example.com", default_model="gemini-2.5-flash")
    mock_client = _patched_client(GEMINI_OK)
    with patch("httpx.Client") as cls:
        cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        cls.return_value.__exit__  = MagicMock(return_value=False)
        adapter.analyze(TR, [], "gemini-2.5-pro")
    url = mock_client.post.call_args[0][0]
    assert "gemini-2.5-pro" in url


def test_analyze_sends_correct_generation_config():
    adapter     = GeminiLlmAdapter(api_key="k", base_url="https://api.example.com", default_model="gemini-2.5-flash")
    mock_client = _patched_client(GEMINI_OK)
    with patch("httpx.Client") as cls:
        cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        cls.return_value.__exit__  = MagicMock(return_value=False)
        adapter.analyze(TR, [], None)
    payload = mock_client.post.call_args[1]["json"]
    cfg     = payload["generationConfig"]
    assert cfg["temperature"]      == 0.2
    assert cfg["maxOutputTokens"]  == 8192


def test_build_prompt_limits_to_200_per_source():
    logs   = [_log("slowlog", "SLOWLOG", i) for i in range(250)]
    prompt = _build_prompt(TR, logs)
    lines  = [l for l in prompt.split("\n") if "msg" in l]
    assert len(lines) == 200


def test_build_prompt_groups_by_source():
    logs   = [_log("slowlog", "SLOWLOG"), _log("es_query_log", "SUCCESS"), _log("node_metric", "METRIC")]
    prompt = _build_prompt(TR, logs)
    assert "[slowlog]"      in prompt
    assert "[es_query_log]" in prompt
    assert "[node_metric]"  in prompt


def test_build_prompt_contains_analysis_principles():
    prompt = _build_prompt(TR, [])
    assert "임계치 초과" in prompt
    assert "특이사항 없음" in prompt
    assert "FAIL" in prompt
    assert "토큰 분석 이슈" in prompt
    assert "광범위 범위 검색" in prompt


def test_build_prompt_contains_response_format():
    prompt = _build_prompt(TR, [])
    assert "마크다운 금지" in prompt
    assert "클러스터 건강 상태" in prompt
    assert "요청 이해" in prompt
