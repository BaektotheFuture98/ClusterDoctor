"""LLM 호출 한 건의 시작과 끝을 같은 형식으로 남긴다.

호출 경로가 둘이다. 구조화 응답 한 번을 받는 ``litellm_client.complete()``와,
DeepAgent의 tool loop를 도는 ChatLiteLLM. litellm 자체 로그는 두 경로 모두
"completion() 시작 / Completed Call" 두 줄뿐이라 어느 호출인지 알 수 없었다.
여기서 호출마다 번호·용도·모델·소요 시간·토큰 수를 한 줄로 남긴다.

로그 메시지는 ASCII로 쓴다(Windows cp949 stderr). 프롬프트·응답 본문·URL은
싣지 않는다 — 용도 라벨과 수치만 남긴다.
"""

from __future__ import annotations

import itertools
import logging
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any
from uuid import UUID

from langchain.agents.middleware import AgentMiddleware
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.messages import AIMessage, ToolMessage

# litellm이 호출마다 INFO 두 줄을 찍는다. 아래 로그가 같은 정보를 용도와 함께
# 담으므로 중복을 끈다. WARNING 이상은 그대로 남는다.
logging.getLogger("LiteLLM").setLevel(logging.WARNING)

_logger = logging.getLogger(__name__)

_call_ids = itertools.count(1)
_label: ContextVar[str] = ContextVar("llm_call_label", default="")


@contextmanager
def llm_label(text: str) -> Iterator[None]:
    """이 블록 안에서 일어나는 LLM 호출에 용도 라벨을 붙인다."""
    token = _label.set(text)
    try:
        yield
    finally:
        _label.reset(token)


class LlmCallTrace:
    """호출 한 건. 생성하면 시작 로그가, done/failed로 끝 로그가 나간다."""

    def __init__(self, source: str, model: str, label: str | None = None) -> None:
        self.call_id = next(_call_ids)
        self.source = source
        self.model = model
        self.label = label if label is not None else _label.get()
        self._started = time.monotonic()
        _logger.info(
            "LLM #%d start [%s] %s model=%s",
            self.call_id,
            self.source,
            self.label or "-",
            self.model,
        )

    def done(
        self,
        *,
        prompt_tokens: int | None = None,
        completion_tokens: int | None = None,
        finish_reason: str | None = None,
        tool_calls: Sequence[str] = (),
    ) -> None:
        extra = f" tool_calls={list(tool_calls)}" if tool_calls else ""
        _logger.info(
            "LLM #%d done [%s] %s %.1fs tokens(in=%s out=%s) finish=%s%s",
            self.call_id,
            self.source,
            self.label or "-",
            time.monotonic() - self._started,
            prompt_tokens if prompt_tokens is not None else "?",
            completion_tokens if completion_tokens is not None else "?",
            finish_reason or "?",
            extra,
        )

    def failed(self, reason: str) -> None:
        _logger.warning(
            "LLM #%d failed [%s] %s %.1fs reason=%s",
            self.call_id,
            self.source,
            self.label or "-",
            time.monotonic() - self._started,
            reason,
        )


class AgentCallLogHandler(BaseCallbackHandler):
    """DeepAgent(ChatLiteLLM) 경로의 호출을 ``LlmCallTrace``로 남긴다.

    Main과 SubAgent가 같은 모델 객체를 쓰므로 호출자는 ``AgentRoleLogMiddleware``가
    붙인 라벨로 구분하고, 모델이 고른 tool 이름은 끝 로그에 남긴다.
    """

    def __init__(self, model: str) -> None:
        self._model = model
        self._traces: dict[UUID, LlmCallTrace] = {}

    def on_chat_model_start(
        self, serialized: dict[str, Any], messages: Any, *, run_id: UUID, **kwargs: Any
    ) -> None:
        self._traces[run_id] = LlmCallTrace("agent", self._model, _label.get() or "agent step")

    def on_llm_end(self, response: Any, *, run_id: UUID, **kwargs: Any) -> None:
        trace = self._traces.pop(run_id, None)
        if trace is None:
            return
        usage = (response.llm_output or {}).get("token_usage") or {}
        tool_calls: list[str] = []
        finish = None
        for generations in response.generations:
            for generation in generations:
                message = getattr(generation, "message", None)
                tool_calls += [c.get("name", "?") for c in getattr(message, "tool_calls", [])]
                finish = finish or (generation.generation_info or {}).get("finish_reason")
        trace.done(
            prompt_tokens=usage.get("prompt_tokens"),
            completion_tokens=usage.get("completion_tokens"),
            finish_reason=finish,
            tool_calls=tool_calls,
        )

    def on_llm_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        trace = self._traces.pop(run_id, None)
        if trace is not None:
            # str(error)는 본문·URL이 실릴 수 있어 타입 이름만 남긴다.
            trace.failed(type(error).__name__)


def _last_tool_name(messages: Sequence[Any]) -> str | None:
    """직전 메시지가 tool 결과면 그 tool의 이름. 대화 처음이면 None."""
    if not messages or not isinstance(messages[-1], ToolMessage):
        return None
    last = messages[-1]
    if getattr(last, "name", None):
        return last.name
    for message in reversed(messages[:-1]):
        if isinstance(message, AIMessage):
            for call in message.tool_calls:
                if call.get("id") == last.tool_call_id:
                    return call.get("name")
    return "?"


class AgentRoleLogMiddleware(AgentMiddleware):
    """모델 호출에 ``main`` / ``subagent`` 역할과 직전 tool 결과를 라벨로 붙인다.

    라벨은 예를 들어 ``main (after propose_analysis)``이고, 대화 첫 호출은
    ``main (start)``다. 어떤 tool을 고르는 호출인지는 끝 로그의 ``tool_calls``에
    나온다.
    """

    def __init__(self, role: str) -> None:
        super().__init__()
        self._role = role

    def _label(self, request: Any) -> str:
        after = _last_tool_name(request.messages)
        return f"{self._role} ({'after ' + after if after else 'start'})"

    def wrap_model_call(self, request: Any, handler: Callable[[Any], Any]) -> Any:
        with llm_label(self._label(request)):
            return handler(request)

    async def awrap_model_call(
        self, request: Any, handler: Callable[[Any], Any]
    ) -> Any:
        with llm_label(self._label(request)):
            return await handler(request)
