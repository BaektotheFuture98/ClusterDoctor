"""Main DeepAgent가 쓰는 chat model.

``litellm_client.complete()``는 왕복 한 번에 텍스트 하나를 돌려주는 함수라
tool loop를 돌 수 없다. 그래서 DeepAgent 쪽에 두 번째 경로가 생겼고, **경로가
갈라지면 한쪽에서만 보호 장치가 사라지는 사고**가 난다. 여기서 보는 것이
그것이다 — 재시도가 정말로 꺼져 있는가.

재시도는 이 저장소에서 실측된 사고다. 429의 원인은 분당 입력 토큰 한도 초과이고,
같은 프롬프트를 다시 보내면 실패가 보장된 채 소비만 배로 늘어난다
(513,122 토큰 → 재시도 포함 2,052,488 토큰, 한도 250,000의 821%).
"""

import pytest

from cluster_doctor.infrastructure.config.settings import ConfigurationError
from cluster_doctor.infrastructure.outbound.agent.common.litellm_client import (
    _REQUEST_TIMEOUT_SECONDS,
)
from cluster_doctor.infrastructure.outbound.agent.supervisor import chat_model


def build(provider="gemini", model="gemini-3.5-flash-lite"):
    return chat_model.build_chat_model(provider=provider, model=model, api_key="k")


class TestRetriesAreOff:
    def test_langchain_계층의_재시도가_꺼져_있다(self):
        """``max_retries``의 기본값은 1이다. **끄지 않으면 켜져 있다.**"""
        assert build().max_retries == 0

    def test_litellm_계층의_재시도도_함께_꺼져_있다(self):
        """``max_retries``와 별개의 층이고 ``ChatLiteLLM``이 건드리지 않는다.
        한 층만 끄면 나머지 한 층이 같은 증폭을 그대로 일으킨다."""
        assert build().model_kwargs.get("num_retries") == 0

    def test_요청_타임아웃이_단일_경로와_같은_값이다(self):
        """두 경로가 다른 값을 쓰면 한쪽만 고쳐지는 날이 온다."""
        assert build().request_timeout == _REQUEST_TIMEOUT_SECONDS

    def test_샘플링_인자를_보내지_않는다(self):
        """모델을 바꾸면 그 모델에 맞는 provider 기본값을 따라가야 한다."""
        built = build()
        assert built.temperature is None
        assert built.top_p is None

    def test_provider_접두사가_모델_문자열에_붙는다(self):
        assert build("nvidia_nim", "google/gemma-4-31b-it").model == (
            "nvidia_nim/google/gemma-4-31b-it"
        )


class TestToolCallingGuard:
    def test_tool_call을_못_낸다고_확인된_조합은_기동을_막는다(self, monkeypatch):
        """Main DeepAgent는 tool call 말고는 움직일 방법이 없다. 그 사실을
        첫 Incident를 태운 뒤에 아는 것보다 조립 시점에 아는 쪽이 싸다."""
        monkeypatch.setitem(chat_model.litellm.model_cost, "gemini/no-tools", {})
        monkeypatch.setattr(
            chat_model.litellm, "supports_function_calling", lambda model: False
        )

        with pytest.raises(ConfigurationError, match="tool call"):
            chat_model.require_tool_calling("gemini", "no-tools")

    def test_레지스트리에_없으면_경고만_남기고_통과한다(self, monkeypatch, caplog):
        """항목이 없는 것은 부정이 아니라 침묵이다. litellm의 nvidia_nim 항목은
        reranker 셋뿐이라 우리가 쓰는 모델은 애초에 등재되어 있지 않다 —
        침묵을 부정으로 읽으면 provider 하나가 근거 없이 통째로 막힌다."""

        def explode(model):
            raise AssertionError("레지스트리에 없는 모델을 판정하려 들었다")

        monkeypatch.setattr(chat_model.litellm, "supports_function_calling", explode)

        with caplog.at_level("WARNING"):
            chat_model.require_tool_calling("nvidia_nim", "google/gemma-4-31b-it")

        assert any("tool-call support" in record.message for record in caplog.records)

    def test_실패_메시지에_값이_아니라_설정_이름만_담는다(self, monkeypatch):
        """이 경로가 함께 다루는 값 중 하나가 API 키다. "지금 값은 이것"을
        찍는 습관이 키를 찍는 사고로 이어진다."""
        monkeypatch.setitem(chat_model.litellm.model_cost, "gemini/no-tools", {})
        monkeypatch.setattr(
            chat_model.litellm, "supports_function_calling", lambda model: False
        )

        with pytest.raises(ConfigurationError) as excinfo:
            chat_model.require_tool_calling("gemini", "no-tools")

        message = str(excinfo.value)
        assert "llm_provider" in message
        assert "no-tools" not in message

    def test_알_수_없는_provider는_여기서도_막힌다(self):
        with pytest.raises(Exception):
            chat_model.require_tool_calling("openai", "gpt-4")
