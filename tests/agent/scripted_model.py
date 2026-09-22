"""대본대로 tool call을 내는 가짜 chat model.

DeepAgent는 **tool call로만 움직인다.** 그래서 모델을 통째로 patch해 버리면
검증할 것이 남지 않는다 — Guardrail 미들웨어도, ``task`` 도구도, state 전달도
전부 langchain/deepagents의 tool loop 위에 서 있기 때문이다. 여기서 바꾸는
것은 **모델 한 겹**뿐이고, 그 아래 그래프는 운영에서 도는 것과 같은 것이다.

``bind_tools``를 덮는 이유는 둘이다.

1. ``FakeMessagesListChatModel``은 ``BaseChatModel``의 기본 구현을 그대로
   물려받아 ``NotImplementedError``를 던진다. 덮지 않으면 그래프가 서지 않는다.
2. 덮는 김에 **모델에게 실제로 보인 도구 이름**을 기록한다. 파일시스템·셸
   도구가 목록에 되살아나는 사고는 그것 말고 드러날 자리가 없다.

대본이 떨어지면 도구 없는 답으로 끝낸다. ``FakeMessagesListChatModel``의
기본 동작은 처음으로 **되감기**라, 대본이 모자라면 같은 도구 호출을 영원히
반복하며 테스트가 재귀 상한에 걸릴 때까지 돈다.
"""

from __future__ import annotations

import itertools
from typing import Any

from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult

EXHAUSTED_TEXT = "대본이 끝났다"

_CALL_IDS = itertools.count(1)


class ScriptedChatModel(FakeMessagesListChatModel):
    """대본 순서대로 답한다. 되감지 않는다."""

    # 호출마다 모델에게 보인 도구 이름. 한 Incident 안에서 Main과 Diagnosis가
    # 각각 자기 도구로 bind되므로 목록의 목록이다.
    bound_tool_names: list[list[str]] = []

    def bind_tools(self, tools: Any, **_kwargs: Any) -> "ScriptedChatModel":
        self.bound_tool_names.append(
            sorted(_tool_name(item) for item in tools)
        )
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        if self.i >= len(self.responses):
            message: Any = AIMessage(content=EXHAUSTED_TEXT)
        else:
            message = self.responses[self.i]
            self.i += 1
        return ChatResult(generations=[ChatGeneration(message=message)])

    @property
    def call_count(self) -> int:
        return self.i


def _tool_name(item: Any) -> str:
    name = getattr(item, "name", None)
    if isinstance(name, str):
        return name
    if isinstance(item, dict):
        return str(item.get("name", item))
    return str(item)


def ai(*calls: tuple[str, dict], content: str = "") -> AIMessage:
    """tool call을 담은 AIMessage 하나.

    한 턴에 여러 개를 낼 수 있게 열어 둔다 — 모델이 실제로 그렇게 하고,
    예산 선예약이 필요한 이유가 바로 그것이다.

    **id는 전역으로 유일해야 한다.** langchain의 라우팅은 "이 tool_call에
    대한 ToolMessage가 이미 있는가"를 id로 판정한다. 대본 안에서 id가 겹치면
    두 번째 호출이 "이미 응답된 것"으로 읽혀 도구가 아예 실행되지 않고,
    그래프는 존재하지 않는 목적지로 라우팅하다 ``KeyError``로 죽는다.
    """
    return AIMessage(
        content=content,
        tool_calls=[
            {"name": name, "args": args, "id": f"call-{next(_CALL_IDS)}"}
            for name, args in calls
        ],
    )


def say(text: str = "끝났다") -> AIMessage:
    """도구를 부르지 않는 답. 이것이 나오면 그 루프는 거기서 끝난다."""
    return AIMessage(content=text)
