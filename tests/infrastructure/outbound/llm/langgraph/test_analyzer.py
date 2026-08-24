"""LangGraphAnalyzer가 포트를 지키고 자격증명을 올바르게 묶는지."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from cluster_doctor.infrastructure.outbound.llm.langgraph.analyzer import LangGraphAnalyzer
from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.application.port.outbound.llm_analyzer import LlmAnalyzer

TR = TimeRange(start=datetime(2026, 8, 20, 2, 9), end=datetime(2026, 8, 20, 2, 11))
CANARY_KEY = "sk-CANARY-DO-NOT-LOG-12345"

LOGS = [
    LogEntry(
        timestamp=datetime(2026, 8, 20, 2, 9, 5),
        level="SLOWLOG",
        source="slowlog",
        message="probe-marker",
    )
]


def _response(content, finish_reason="stop"):
    choice = MagicMock()
    choice.message.content = content
    choice.finish_reason = finish_reason
    resp = MagicMock()
    resp.choices = [choice]
    return resp


def _adapter(provider="nvidia", model="meta/llama-3.3-70b-instruct"):
    return LangGraphAnalyzer(
        provider=provider, api_key=CANARY_KEY, default_model=model
    )


def test_implements_the_llm_analyzer_port():
    assert issubclass(LangGraphAnalyzer, LlmAnalyzer)
    assert not LangGraphAnalyzer.__abstractmethods__


def test_unknown_provider_is_rejected_at_construction():
    with pytest.raises(ValueError, match="provider"):
        LangGraphAnalyzer(provider="openai", api_key="k", default_model="gpt-4")


def test_returns_the_synthesis_text():
    with patch("litellm.completion", return_value=_response("최종 리포트")):
        assert _adapter().analyze(TR, LOGS, None) == "최종 리포트"


def test_every_call_carries_the_provider_prefix_and_key():
    """구간별 호출과 종합 호출 모두 같은 provider·키로 나가야 한다."""
    with patch("litellm.completion", return_value=_response("ok")) as call:
        _adapter().analyze(TR, LOGS, None)

    assert call.call_count >= 2, "분별 최소 1회 + 종합 1회"
    for kwargs in (c.kwargs for c in call.call_args_list):
        assert kwargs["model"] == "nvidia_nim/meta/llama-3.3-70b-instruct"
        assert kwargs["api_key"] == CANARY_KEY
        assert CANARY_KEY not in kwargs["model"]


def test_caller_supplied_model_overrides_the_default_everywhere():
    with patch("litellm.completion", return_value=_response("ok")) as call:
        _adapter(provider="gemini", model="gemini-2.5-flash").analyze(
            TR, LOGS, "gemini-2.5-pro"
        )

    models = {c.kwargs["model"] for c in call.call_args_list}
    assert models == {"gemini/gemini-2.5-pro"}


def test_minute_calls_use_a_smaller_token_budget_than_the_synthesis():
    """분별 요약은 짧으면 충분하고, 한도가 낮으면 length 절단도 덜 난다."""
    with patch("litellm.completion", return_value=_response("ok")) as call:
        _adapter().analyze(TR, LOGS, None)

    budgets = [c.kwargs["max_tokens"] for c in call.call_args_list]
    assert min(budgets) < max(budgets)
    assert max(budgets) == 8192
