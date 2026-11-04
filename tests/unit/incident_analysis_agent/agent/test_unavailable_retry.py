"""503/429 재시도 정책: 대기 시간 계산과 두 호출 경로의 재시도 횟수."""

from types import SimpleNamespace

import httpx
import pytest
from litellm.exceptions import RateLimitError, ServiceUnavailableError

from cluster_doctor.exceptions import LlmApiError
from cluster_doctor.incident_analysis_agent.agent.runtime import (
    litellm_client,
    rate_limit_retry,
)
from cluster_doctor.incident_analysis_agent.agent.runtime.litellm_client import (
    _retry_wait,
)
from cluster_doctor.incident_analysis_agent.agent.runtime.rate_limit_retry import (
    RateLimitRetryMiddleware,
)

_TOKEN_QUOTA_TEXT = (
    '{"quotaId": "GenerateContentInputTokensPerModelPerMinute", "quotaValue": "250000"}'
)


def _error(cls, status, message="busy", retry_after=None):
    response = None
    if retry_after is not None:
        response = httpx.Response(
            status,
            headers={"retry-after": str(retry_after)},
            request=httpx.Request("POST", "http://provider.invalid"),
        )
    return cls(message=message, llm_provider="gemini", model="m", response=response)


def _unavailable(**kwargs):
    return _error(ServiceUnavailableError, 503, **kwargs)


def _rate_limited(**kwargs):
    return _error(RateLimitError, 429, **kwargs)


class TestRetryPolicy:
    def test_attempt_ceiling_covers_both_retry_loops(self):
        assert litellm_client._MAX_ATTEMPTS == 4

    @pytest.mark.parametrize(
        ("status", "limit"), [(503, 3), (429, 2), (500, 0), (400, 0), ("unknown", 0)]
    )
    def test_retry_limit(self, status, limit):
        assert litellm_client._retry_limit(status) == limit

    @pytest.mark.parametrize(
        ("attempt", "wait"), [(0, 15.0), (1, 30.0), (2, 60.0), (3, None)]
    )
    def test_503_without_provider_delay_backs_off_per_attempt(self, attempt, wait):
        assert _retry_wait(_unavailable(), 503, attempt) == wait

    @pytest.mark.parametrize("attempt", [0, 1, 2])
    def test_503_retry_after_header_adds_margin(self, attempt):
        assert _retry_wait(_unavailable(retry_after=7), 503, attempt) == 8.0

    def test_503_body_retry_delay_adds_margin(self):
        exc = _unavailable(message='{"retryDelay": "7s"}')
        assert _retry_wait(exc, 503, 0) == 8.0

    def test_503_provider_delay_over_cap_gives_up(self):
        assert _retry_wait(_unavailable(retry_after=61), 503, 0) is None
        exc = _unavailable(message='{"retryDelay": "90s"}')
        assert _retry_wait(exc, 503, 0) is None

    def test_503_provider_delay_still_bounded_by_attempt_limit(self):
        assert _retry_wait(_unavailable(retry_after=7), 503, 3) is None

    @pytest.mark.parametrize(("attempt", "wait"), [(0, 5.0), (1, 5.0), (2, None)])
    def test_429_without_provider_delay_keeps_fixed_backoff(self, attempt, wait):
        assert _retry_wait(_rate_limited(), 429, attempt) == wait

    def test_429_token_quota_without_delay_is_not_retried(self):
        exc = _rate_limited(message=_TOKEN_QUOTA_TEXT)
        assert _retry_wait(exc, 429, 0) is None

    def test_429_provider_delay_adds_margin(self):
        assert _retry_wait(_rate_limited(retry_after=7), 429, 0) == 8.0


@pytest.fixture
def sleeps(monkeypatch):
    waits: list[float] = []
    monkeypatch.setattr(litellm_client.time, "sleep", waits.append)
    monkeypatch.setattr(litellm_client, "_throttle", lambda provider: None)
    return waits


def _fake_response():
    return SimpleNamespace(
        choices=[
            SimpleNamespace(message=SimpleNamespace(content="ok"), finish_reason="stop")
        ],
        usage=SimpleNamespace(prompt_tokens=1, completion_tokens=1),
    )


def _script_completion(monkeypatch, failures, error_factory):
    """앞 failures번은 예외를 던지고 그 뒤로는 응답을 돌려주는 가짜 completion."""
    calls: list[dict] = []

    def fake_completion(**kwargs):
        calls.append(kwargs)
        if len(calls) <= failures:
            raise error_factory()
        return _fake_response()

    monkeypatch.setattr(litellm_client.litellm, "completion", fake_completion)
    return calls


def _complete():
    return litellm_client.complete(
        [{"role": "user", "content": "hi"}], "gemini", "m", "key"
    )


class TestCompleteRetries:
    def test_503_three_times_then_success(self, monkeypatch, sleeps):
        calls = _script_completion(monkeypatch, 3, _unavailable)

        assert _complete() == "ok"
        assert sleeps == [15.0, 30.0, 60.0]
        assert len(calls) == 4

    def test_503_four_times_raises(self, monkeypatch, sleeps):
        calls = _script_completion(monkeypatch, 99, _unavailable)

        with pytest.raises(LlmApiError):
            _complete()
        assert len(calls) == 4
        assert sleeps == [15.0, 30.0, 60.0]

    def test_429_twice_then_success(self, monkeypatch, sleeps):
        calls = _script_completion(monkeypatch, 2, _rate_limited)

        assert _complete() == "ok"
        assert sleeps == [5.0, 5.0]
        assert len(calls) == 3

    def test_429_three_times_raises(self, monkeypatch, sleeps):
        calls = _script_completion(monkeypatch, 99, _rate_limited)

        with pytest.raises(LlmApiError):
            _complete()
        assert len(calls) == 3
        assert sleeps == [5.0, 5.0]


class _Handler:
    """앞 failures번은 예외를 던지고 그 뒤로는 "ok"를 돌려주는 핸들러."""

    def __init__(self, failures, error_factory):
        self.failures = failures
        self.error_factory = error_factory
        self.calls = 0
        self.last_error = None

    def _step(self):
        self.calls += 1
        if self.calls <= self.failures:
            self.last_error = self.error_factory()
            raise self.last_error
        return "ok"

    def __call__(self, request):
        return self._step()


class _AsyncHandler(_Handler):
    async def __call__(self, request):
        return self._step()


class TestMiddlewareRetries:
    def test_sync_503_three_times_then_success(self, monkeypatch):
        waits: list[float] = []
        monkeypatch.setattr(rate_limit_retry.time, "sleep", waits.append)
        handler = _Handler(3, _unavailable)

        result = RateLimitRetryMiddleware().wrap_model_call(object(), handler)

        assert result == "ok"
        assert waits == [15.0, 30.0, 60.0]

    def test_sync_503_four_times_propagates_original(self, monkeypatch):
        waits: list[float] = []
        monkeypatch.setattr(rate_limit_retry.time, "sleep", waits.append)
        handler = _Handler(99, _unavailable)

        with pytest.raises(ServiceUnavailableError) as info:
            RateLimitRetryMiddleware().wrap_model_call(object(), handler)

        assert info.value is handler.last_error
        assert handler.calls == 4

    async def test_async_503_three_times_then_success(self, monkeypatch):
        waits: list[float] = []

        async def fake_sleep(seconds):
            waits.append(seconds)

        monkeypatch.setattr(rate_limit_retry.asyncio, "sleep", fake_sleep)
        handler = _AsyncHandler(3, _unavailable)

        result = await RateLimitRetryMiddleware().awrap_model_call(object(), handler)

        assert result == "ok"
        assert waits == [15.0, 30.0, 60.0]

    async def test_async_503_four_times_propagates_original(self, monkeypatch):
        async def fake_sleep(seconds):
            pass

        monkeypatch.setattr(rate_limit_retry.asyncio, "sleep", fake_sleep)
        handler = _AsyncHandler(99, _unavailable)

        with pytest.raises(ServiceUnavailableError) as info:
            await RateLimitRetryMiddleware().awrap_model_call(object(), handler)

        assert info.value is handler.last_error
        assert handler.calls == 4
