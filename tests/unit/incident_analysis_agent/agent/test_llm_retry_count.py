"""429/503 재시도 횟수: 일반 호출과 에이전트 호출이 같은 상한을 쓰고 상한에서 멈춘다."""
import logging

import httpx
import openai
import pytest

from cluster_doctor.exceptions import LlmApiError
from cluster_doctor.incident_analysis_agent.agent.runtime import litellm_client, rate_limit_retry

RETRIES = 2


def _unavailable() -> openai.InternalServerError:
    request = httpx.Request("POST", "https://llm.invalid/v1")
    return openai.InternalServerError("unavailable", response=httpx.Response(503, request=request), body=None)


def test_retry_limit_is_two():
    assert litellm_client._MAX_RETRIES == RETRIES


def test_complete_retries_twice_then_raises(monkeypatch, caplog):
    calls = []

    def failing_completion(**kwargs):
        calls.append(kwargs)
        raise _unavailable()

    monkeypatch.setattr(litellm_client.litellm, "completion", failing_completion)
    monkeypatch.setattr(litellm_client.time, "sleep", lambda seconds: None)

    with caplog.at_level(logging.WARNING, logger=litellm_client._logger.name):
        with pytest.raises(LlmApiError):
            litellm_client.complete([{"role": "user", "content": "q"}], "nvidia_nim", "model", "key")

    assert len(calls) == RETRIES + 1
    retry_logs = [r.getMessage() for r in caplog.records if "retrying in" in r.getMessage()]
    assert len(retry_logs) == RETRIES
    assert retry_logs[0].endswith("(1/2)") and retry_logs[1].endswith("(2/2)")


def test_complete_stops_retrying_after_a_success(monkeypatch):
    class Response:
        class _Choice:
            finish_reason = "stop"

            class message:
                content = "ok"

        choices = [_Choice()]
        usage = None

    outcomes = iter([_unavailable(), Response()])

    def flaky_completion(**kwargs):
        outcome = next(outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(litellm_client.litellm, "completion", flaky_completion)
    monkeypatch.setattr(litellm_client.time, "sleep", lambda seconds: None)

    assert litellm_client.complete([{"role": "user", "content": "q"}], "nvidia_nim", "model", "key") == "ok"


def test_agent_middleware_retries_twice_then_raises(monkeypatch):
    attempts = []
    monkeypatch.setattr(rate_limit_retry.time, "sleep", lambda seconds: None)

    def handler(request):
        attempts.append(request)
        raise _unavailable()

    with pytest.raises(openai.InternalServerError):
        rate_limit_retry.RateLimitRetryMiddleware().wrap_model_call("request", handler)

    assert len(attempts) == RETRIES + 1
