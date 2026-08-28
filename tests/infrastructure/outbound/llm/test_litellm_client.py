"""``complete``가 litellm에 넘기는 인자를 고정한다.

여기서 검증하는 것은 응답 처리가 아니라 *요청 조립*이다. 호출 인자는
provider 동작을 직접 바꾸는데, 잘못 바뀌어도 예외가 나지 않고 리포트 품질만
조용히 달라진다. 그래서 인자 하나하나를 명시적으로 못 박는다.
"""

from unittest.mock import MagicMock, patch

import pytest

from cluster_doctor.application.port.outbound.llm_analyzer import LlmResponseError
from cluster_doctor.infrastructure.outbound.llm.litellm_client import complete

MESSAGES = [{"role": "user", "content": "안녕"}]


def _ok_response(text="리포트"):
    choice = MagicMock()
    choice.message.content = text
    choice.finish_reason = "stop"
    response = MagicMock()
    response.choices = [choice]
    return response


def _call(**overrides):
    kwargs = {
        "messages": MESSAGES,
        "provider": "gemini",
        "model": "gemini-2.5-flash",
        "api_key": "test-key",
    }
    kwargs.update(overrides)
    with patch("litellm.completion", return_value=_ok_response()) as completion:
        complete(**kwargs)
    return completion.call_args.kwargs


def test_temperature_is_not_sent():
    # 명시하지 않고 provider 기본값을 따른다.
    assert "temperature" not in _call()


def test_top_p_and_other_sampling_knobs_are_not_sent_either():
    # temperature만 빼고 다른 샘플링 인자가 슬쩍 들어오면 같은 문제가 반복된다.
    sent = _call()
    for knob in ("top_p", "top_k", "presence_penalty", "frequency_penalty", "seed"):
        assert knob not in sent, knob


def test_request_still_carries_the_parameters_that_must_survive():
    # temperature를 걷어내면서 나머지가 함께 지워지지 않았는지 확인한다.
    sent = _call()
    assert sent["model"] == "gemini/gemini-2.5-flash"
    assert sent["messages"] == MESSAGES
    assert sent["api_key"] == "test-key"
    assert sent["max_tokens"] == 8192
    assert sent["timeout"] == 120.0
    assert sent["num_retries"] == 3


def test_api_key_is_never_folded_into_the_model_string():
    # 키가 모델 문자열이나 URL에 실리면 provider 에러 메시지·로그로 새어 나간다.
    assert "test-key" not in _call()["model"]


def test_response_format_is_sent_only_when_asked():
    assert "response_format" not in _call()
    assert _call(response_format=dict)["response_format"] is dict


def test_max_tokens_is_caller_controlled():
    assert _call(max_tokens=1024)["max_tokens"] == 1024


def test_empty_text_becomes_a_response_error():
    response = _ok_response(text="")
    response.choices[0].finish_reason = "length"
    with patch("litellm.completion", return_value=response):
        with pytest.raises(LlmResponseError, match="토큰 한도"):
            complete(
                messages=MESSAGES,
                provider="gemini",
                model="gemini-2.5-flash",
                api_key="test-key",
            )
