"""``complete``가 litellm에 넘기는 인자를 고정한다.

여기서 검증하는 것은 응답 처리가 아니라 *요청 조립*이다. 호출 인자는
provider 동작을 직접 바꾸는데, 잘못 바뀌어도 예외가 나지 않고 리포트 품질만
조용히 달라진다. 그래서 인자 하나하나를 명시적으로 못 박는다.
"""

from unittest.mock import MagicMock, patch

import pytest

from cluster_doctor.application.port.outbound.llm_analyzer import LlmResponseError
from cluster_doctor.infrastructure.outbound.llm.litellm_client import (
    complete,
    require_supported_provider,
)

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
    assert sent["num_retries"] == 0


def test_retries_are_disabled():
    # 429는 분당 입력 토큰 한도 초과로 난다. 같은 프롬프트를 다시 보내면
    # 실패가 보장된 채 소비만 배로 늘어난다(실측 4배). litellm은
    # completion()에 오류 종류별 재시도 정책을 받지 않으므로 전부 끈다.
    assert _call()["num_retries"] == 0


def test_api_key_is_never_folded_into_the_model_string():
    # 키가 모델 문자열이나 URL에 실리면 provider 에러 메시지·로그로 새어 나간다.
    assert "test-key" not in _call()["model"]


def test_response_format_is_sent_only_when_asked():
    assert "response_format" not in _call()
    assert _call(response_format=dict)["response_format"] is dict


def test_max_tokens_is_caller_controlled():
    assert _call(max_tokens=1024)["max_tokens"] == 1024


def test_nvidia_nim_provider_is_supported():
    assert require_supported_provider("nvidia_nim") == "nvidia_nim"


def test_nvidia_model_gets_the_nvidia_prefix():
    # 모델명 자체에 슬래시가 있다("google/gemma-4-31b-it"). prefix를 붙인
    # 결과가 "nvidia_nim/google/gemma-4-31b-it"여야 litellm이 라우팅한다.
    sent = _call(provider="nvidia_nim", model="google/gemma-4-31b-it")
    assert sent["model"] == "nvidia_nim/google/gemma-4-31b-it"
    # 키는 여전히 파라미터로만 간다.
    assert "test-key" not in sent["model"]


def test_unsupported_provider_names_what_is_supported():
    # 오타를 첫 호출까지 끌고 가지 않는다. 메시지가 지원 목록을 알려줘야
    # 운영자가 무엇을 적어야 하는지 안다.
    with pytest.raises(ValueError, match="nvidia_nim"):
        require_supported_provider("nvidia")


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


# --------------------------------------------------------------------------
# provider 거절(429) 진단 로깅
#
# LlmApiError는 상태 코드만 담는다(본문·URL에 키가 실릴 수 있어서). 그 결과
# 429가 났을 때 "요청 수 한도인지 토큰 한도인지" 알 길이 없었다. 대응이 서로
# 다르므로(호출 간격 vs 프롬프트 크기) 구분에 필요한 헤더만 로그로 남긴다.
# --------------------------------------------------------------------------

def _rate_limited(headers=None, **attrs):
    """429를 던지는 openai.APIError 대역."""
    import openai

    exc = openai.APIError.__new__(openai.APIError)
    exc.status_code = 429
    exc.response = MagicMock()
    exc.response.headers = headers if headers is not None else {}
    for name, value in attrs.items():
        setattr(exc, name, value)
    return exc


def _complete_expecting_failure(exc, caplog):
    from cluster_doctor.application.port.outbound.llm_analyzer import LlmApiError

    with patch("litellm.completion", side_effect=exc):
        with caplog.at_level("WARNING"):
            with pytest.raises(LlmApiError):
                complete(
                    messages=MESSAGES,
                    provider="nvidia_nim",
                    model="google/gemma-4-31b-it",
                    api_key="test-key",
                )
    return caplog.text


def test_429_logs_the_token_limit_headers(caplog):
    # TPM 초과인지 알려 주는 값들. 프롬프트 크기를 줄여야 하는 경우다.
    text = _complete_expecting_failure(
        _rate_limited(
            headers={
                "retry-after": "40",
                "x-ratelimit-limit-tokens": "250000",
                "x-ratelimit-remaining-tokens": "0",
            }
        ),
        caplog,
    )

    assert "status=429" in text
    assert "retry-after=40" in text
    assert "x-ratelimit-limit-tokens=250000" in text
    assert "x-ratelimit-remaining-tokens=0" in text


def test_429_logs_the_request_limit_headers(caplog):
    # RPM 초과인지 알려 주는 값들. 호출 간격을 벌려야 하는 경우다.
    text = _complete_expecting_failure(
        _rate_limited(
            headers={
                "x-ratelimit-limit-requests": "60",
                "x-ratelimit-remaining-requests": "0",
            }
        ),
        caplog,
    )

    assert "x-ratelimit-limit-requests=60" in text


def test_provider_error_logs_the_short_machine_code(caplog):
    text = _complete_expecting_failure(
        _rate_limited(code="rate_limit_exceeded", type="requests"),
        caplog,
    )

    assert "code=rate_limit_exceeded" in text
    assert "type=requests" in text


def test_never_logs_the_response_body_or_the_request_url(caplog):
    # 이 모듈 전체의 전제다. URL에는 provider에 따라 키가 실린다.
    exc = _rate_limited(
        headers={"retry-after": "40", "authorization": "Bearer super-secret"},
        body={"error": {"message": "quota exceeded for https://host/v1?key=SECRET"}},
    )
    exc.request = MagicMock()
    exc.request.url = "https://host/v1/chat/completions?key=SECRET"

    text = _complete_expecting_failure(exc, caplog)

    assert "SECRET" not in text
    assert "super-secret" not in text
    assert "https://" not in text
    # 화이트리스트에 없는 헤더는 남기지 않는다.
    assert "authorization" not in text.lower()


def test_missing_headers_do_not_break_the_conversion(caplog):
    # provider가 rate-limit 헤더를 보내지 않는 경우가 있다. 그때도 예외
    # 변환은 정상이어야 하고, 헤더가 없다는 사실만 남는다.
    text = _complete_expecting_failure(_rate_limited(headers=None), caplog)

    assert "no rate-limit headers" in text


def test_a_broken_response_object_does_not_mask_the_error(caplog):
    # 진단용 부가 정보가 본래 오류를 가리는 것은 뒤바뀐 우선순위다.
    exc = _rate_limited()
    broken = MagicMock()
    type(broken).headers = property(lambda self: (_ for _ in ()).throw(RuntimeError("boom")))
    exc.response = broken

    text = _complete_expecting_failure(exc, caplog)

    assert "status=429" in text
