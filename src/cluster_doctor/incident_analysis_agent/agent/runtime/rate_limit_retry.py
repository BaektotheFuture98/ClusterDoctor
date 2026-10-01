"""DeepAgent(ChatLiteLLM) 경로의 429/503 재시도.

``litellm_client.complete()``와 같은 정책(``_retry_wait``)을 쓴다. 두 경로의
재시도 규칙이 갈라지면 한쪽만 한도 오류로 죽는다 — Main/SubAgent 호출이 429
한 번에 ``Agent 실행 실패``로 끝나던 원인이다. ChatLiteLLM 자체 재시도는
소비 증폭 때문에 꺼 두었으므로(``chat_model.py``), 여기서 오류 종류를 가려
한 번만 다시 부른다.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from typing import Any

import openai
from langchain.agents.middleware import AgentMiddleware

from cluster_doctor.incident_analysis_agent.agent.runtime.litellm_client import (
    _MAX_RETRIES,
    _retry_wait,
)

_logger = logging.getLogger(__name__)


def _wait_or_raise(exc: openai.APIError, attempt: int) -> float:
    status = getattr(exc, "status_code", None) or "unknown"
    wait = _retry_wait(exc, status, attempt) if attempt < _MAX_RETRIES else None
    if wait is None:
        raise exc
    # str(exc)는 본문·URL이 실릴 수 있어 로그에 싣지 않는다.
    _logger.warning(
        "agent step rejected with status=%s; retrying in %.0fs (%d/%d)",
        status,
        wait,
        attempt + 1,
        _MAX_RETRIES,
    )
    return wait


class RateLimitRetryMiddleware(AgentMiddleware):
    def wrap_model_call(self, request: Any, handler: Callable[[Any], Any]) -> Any:
        for attempt in range(_MAX_RETRIES + 1):
            try:
                return handler(request)
            except openai.APIError as exc:
                time.sleep(_wait_or_raise(exc, attempt))
        raise AssertionError("unreachable")  # pragma: no cover

    async def awrap_model_call(
        self, request: Any, handler: Callable[[Any], Any]
    ) -> Any:
        for attempt in range(_MAX_RETRIES + 1):
            try:
                return await handler(request)
            except openai.APIError as exc:
                await asyncio.sleep(_wait_or_raise(exc, attempt))
        raise AssertionError("unreachable")  # pragma: no cover
