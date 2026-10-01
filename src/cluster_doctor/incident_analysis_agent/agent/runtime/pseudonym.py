"""LLM에 보내는 글의 식별 정보를 가명으로 바꾸고, 응답에서 되돌린다.

대상: IPv4, 이메일, 그리고 수집 단계에서 등록한 회사·사용자·요청 ID.
진단 품질에 직접 쓰이는 값(키워드, 노드 이름, 쿼리 본문 등)은 가리지 않는다.

호출 경로가 둘이라 가명 규칙은 하나로 모은다. 같은 값은 어느 호출에서나 같은
가명(``ip-0001``, ``user-0001`` 등)을 받는다 — 모델이 "같은 노드·같은 사용자"를 구분할 수 있어야 하고,
tool loop에서 이전 대화가 다시 전송될 때도 가명이 어긋나면 안 되기 때문이다.

- ``litellm_client.complete()``: 메시지를 직접 마스킹하고 응답 텍스트를 복원한다.
- DeepAgent(Main/Sub): ``PseudonymizeMiddleware``가 모델 호출 직전과 직후에 같은 일을 한다.

State와 도구는 원래 값을 그대로 다룬다. 가명은 모델 경계 안쪽에만 존재한다.
"""

from __future__ import annotations

import re
import threading
from collections.abc import Callable
from typing import Any

from langchain.agents.middleware import AgentMiddleware

# 문장 끝의 마침표("...172.16.74.61.")는 IP의 일부가 아니다. 뒤따르는 점은 숫자가 이어질
# 때만(1.2.3.4.5) 막는다. 이걸 놓치면 같은 IP가 한쪽에서는 가려지고 다른 쪽에서는
# 그대로 남아 근거 대조가 "없는 IP"로 오판한다.
_IPV4 = re.compile(
    r"(?<!\d)(?<!\d\.)(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)(?!\d)(?!\.\d)"
)
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_PSEUDONYM = re.compile(r"\b(?:ip|email|company|user|req)-\d{4}\b", re.IGNORECASE)

# 이보다 짧은 값은 등록하지 않는다. 한두 글자 사용자 ID가 다른 단어 속에서
# 치환되면 프롬프트가 망가진다.
_MIN_REGISTERED_LENGTH = 3


class Pseudonymizer:
    """값 → 가명 대응표. IP·이메일은 패턴으로, 회사·사용자·요청 ID는 등록된 값으로 찾는다."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._to_alias: dict[str, str] = {}
        self._to_value: dict[str, str] = {}
        self._counters: dict[str, int] = {}
        self._registered: set[str] = set()
        self._registered_pattern: re.Pattern[str] | None = None

    def _alias(self, kind: str, value: str) -> str:
        with self._lock:
            alias = self._to_alias.get(value)
            if alias is None:
                n = self._counters[kind] = self._counters.get(kind, 0) + 1
                alias = f"{kind}-{n:04d}"
                self._to_alias[value] = alias
                self._to_value[alias] = value
            return alias

    def register(self, kind: str, value: object) -> None:
        """수집 단계에서 알게 된 식별 값을 등록한다. 이후 모든 발신 글에서 가려진다."""
        text = str(value or "").strip()
        if len(text) < _MIN_REGISTERED_LENGTH:
            return
        self._alias(kind, text)
        with self._lock:
            if text not in self._registered:
                self._registered.add(text)
                self._registered_pattern = None

    def _pattern(self) -> re.Pattern[str] | None:
        with self._lock:
            if self._registered_pattern is None and self._registered:
                # 긴 값부터 맞춰야 짧은 값이 긴 값의 일부를 먼저 먹지 않는다.
                alternatives = "|".join(
                    re.escape(v) for v in sorted(self._registered, key=len, reverse=True)
                )
                self._registered_pattern = re.compile(
                    rf"(?<!\w)(?:{alternatives})(?!\w)"
                )
            return self._registered_pattern

    def mask(self, text: str) -> str:
        pattern = self._pattern()
        if pattern is not None:
            text = pattern.sub(lambda m: self._to_alias[m.group(0)], text)
        text = _EMAIL.sub(lambda m: self._alias("email", m.group(0)), text)
        return _IPV4.sub(lambda m: self._alias("ip", m.group(0)), text)

    def restore(self, text: str) -> str:
        def back(match: re.Match[str]) -> str:
            with self._lock:
                return self._to_value.get(match.group(0).lower(), match.group(0))

        return _PSEUDONYM.sub(back, text)



def map_value(value: Any, fn: Callable[[str], str]) -> Any:
    """문자열, 리스트, dict 안의 모든 문자열에 ``fn``을 적용한다."""
    if isinstance(value, str):
        return fn(value)
    if isinstance(value, list):
        return [map_value(v, fn) for v in value]
    if isinstance(value, dict):
        return {k: map_value(v, fn) for k, v in value.items()}
    return value


PSEUDONYMS = Pseudonymizer()


def mask_messages(messages: list[dict]) -> list[dict]:
    """``complete()``용. role/content dict 목록의 content를 마스킹한 복사본."""
    return [
        {**m, "content": map_value(m.get("content"), PSEUDONYMS.mask)}
        for m in messages
    ]


def _map_message(message: Any, fn: Callable[[str], str]) -> Any:
    update: dict[str, Any] = {"content": map_value(message.content, fn)}
    tool_calls = getattr(message, "tool_calls", None)
    if tool_calls:
        update["tool_calls"] = map_value(list(tool_calls), fn)
    return message.model_copy(update=update)


class PseudonymizeMiddleware(AgentMiddleware):
    """모델 호출 직전에 IP를 가명으로, 직후에 응답의 가명을 원래 IP로."""

    def wrap_model_call(self, request: Any, handler: Callable[[Any], Any]) -> Any:
        return self._restore(handler(self._masked(request)))

    async def awrap_model_call(
        self, request: Any, handler: Callable[[Any], Any]
    ) -> Any:
        return self._restore(await handler(self._masked(request)))

    @staticmethod
    def _masked(request: Any) -> Any:
        return request.override(
            messages=[_map_message(m, PSEUDONYMS.mask) for m in request.messages]
        )

    @staticmethod
    def _restore(response: Any) -> Any:
        result = getattr(response, "result", None)
        if result is not None:
            response.result = [_map_message(m, PSEUDONYMS.restore) for m in result]
            return response
        if hasattr(response, "content"):
            return _map_message(response, PSEUDONYMS.restore)
        return response
