"""DeepAgent가 기본으로 딸려 보내는 것들을 떼어 낸다.

``create_deep_agent``은 부르기만 하면 파일시스템과 셸 도구를 함께 묶어 준다 —
``ls, read_file, write_file, edit_file, delete, glob, grep, execute``. 범용
코딩 에이전트에는 맞는 기본값이지만 이 저장소에는 맞지 않는다.

**오늘의 실제 폭발 반경은 크지 않다.** 기본 ``StateBackend``는 셸 실행을
아예 지원하지 않아 ``execute``는 "not available"을 돌려주고, ``write_file``은
디스크가 아니라 그래프 state 안의 가상 파일시스템에 쓴다. 그러니 막지 않았을
때 실제로 생기는 일은 호스트 침해가 아니라 state와 컨텍스트가 부푸는 것이다.

그럼에도 막는 이유는 두 가지다. 하나, 이 Agent가 하는 일에 파일은 등장하지
않으므로 도구 목록에 있어 봐야 모델을 헷갈리게 할 뿐이다. 둘, ``backend=``를
한 줄 바꾸는 순간 위 문단이 통째로 거짓이 된다 — 그때 이 차단이 이미 서 있는
것과, 그때부터 찾아 붙이는 것은 다르다.

Main과 Diagnosis 둘 다 ``create_deep_agent``이고, 둘 다 여기를 지나야 한다.
한쪽만 막으면 다른 쪽이 그대로 열려 있고, 실제로 한동안 그랬다.

차단은 두 겹이다.

- ``restrict_harness``  — harness profile로 **모델의 도구 목록에서 지운다.**
  보이지 않는 것은 부를 수 없다. 다만 도구 자체는 ToolNode에 묶인 채 남는다.
- ``DENY_ALL_FILESYSTEM`` — ``permissions``로 **집행한다.** 모델이 목록에 없는
  이름을 지어내 호출해도 여기서 거절된다. deepagents가 이 층을 "security
  guarantee"라고 부른다.

``execute``는 두 번째 겹이 덮지 못한다. ``FilesystemPermission.operations``가
read/write뿐이라 셸은 첫 번째 겹에만 기댄다.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage

from deepagents import (
    FilesystemPermission,
    GeneralPurposeSubagentProfile,
    HarnessProfile,
    register_harness_profile,
)

# deepagents가 harness profile을 찾을 때 쓰는 키 계산기. 밑줄로 시작하지만
# 직접 가져다 쓴다 — 우리가 키를 따로 만들면 deepagents가 조회하는 키와
# 어긋나는 순간 등록이 조용히 무시되고, 조용히 무시된 차단은 아무도 모른다.
from deepagents._models import get_model_identifier, get_model_provider
from langchain_core.language_models.chat_models import BaseChatModel

# 파일시스템 접근을 전부 거절한다. 이 저장소의 Agent가 하는 일에 파일은
# 등장하지 않는다 — 근거도 리포트도 ``ArtifactStore``에 있다.
DENY_ALL_FILESYSTEM = [
    FilesystemPermission(operations=["read", "write"], paths=["/**"], mode="deny")
]

# deepagents 내장 위임 도구 이름. supervisor/tools.py의 ``TASK_TOOL_NAME``과
# 같은 값이지만 그쪽을 import하면 common이 supervisor에 의존하게 된다.
_TASK_TOOL_NAME = "task"

# 모델에게서 감출 도구.
_TOOLS_TO_HIDE = frozenset(
    {"ls", "read_file", "write_file", "edit_file", "delete", "glob", "grep", "execute"}
)


def restrict_harness(model: BaseChatModel) -> None:
    """이 모델로 만들어질 모든 DeepAgent에서 기본 도구와 기본 SubAgent를 뗀다.

    **모델마다 한 번 등록하면 그 뒤 조립 전부에 걸린다.** 등록은 프로세스
    전역이지만 키를 이 모델에 한정하므로 다른 모델의 조립에는 닿지 않는다.
    그래서 호출부는 Agent를 만들기 **전에** 이것을 한 번 부르면 된다.

    general-purpose SubAgent도 함께 끈다. 켜 두면 ``task``의 위임처가 둘이 되고,
    모델이 그쪽을 고르는 순간 Triage도 Evidence 선별도 검증도 없이 만들어진
    무언가가 리포트처럼 돌아온다. 예산은 예산대로 나가고 결과는 믿을 수 없다.
    """
    provider = get_model_provider(model)
    identifier = get_model_identifier(model)
    key = f"{provider}:{identifier}" if provider and identifier else (provider or "")
    if not key:
        # 키를 만들지 못하면 조용히 넘어가지 않는다. 차단이 걸리지 않은 채로
        # 도는 것이 이 함수가 막으려던 바로 그 상황이다.
        raise RuntimeError(
            "chat model에서 harness profile 키를 얻지 못해 "
            "기본 SubAgent와 셸·파일시스템 도구를 끌 수 없습니다."
        )

    register_harness_profile(
        key,
        HarnessProfile(
            general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=False),
            excluded_tools=_TOOLS_TO_HIDE,
        ),
    )


class HideHarnessToolsMiddleware(AgentMiddleware):
    """모델에게 보내는 도구 목록에서 harness 기본 도구를 지운다.

    ``restrict_harness``와 **같은 일을 두 번** 한다. 중복이 아니라 대비다.

    profile 등록은 우리가 만든 키(``f"{provider}:{identifier}"``)와 deepagents가
    조회하는 키가 같을 때만 걸린다. 그 조회에는 fallback이 여럿 있고 provider
    정규화도 들어 있어서, 버전이 오르며 규칙이 바뀌면 등록이 아무도 읽지 않는
    키에 얹힌다. 그때 등록은 예외 없이 무시된다 — deepagents가 남기는 것은
    DEBUG 한 줄뿐이라 운영 로깅 레벨에서는 보이지 않고, ``execute``와
    ``write_file``이 모델 목록에 조용히 돌아온다.

    이쪽은 그 키를 쓰지 않는다. 공개 미들웨어 훅 하나라 deepagents가 내부
    규칙을 바꿔도 같이 깨지지 않는다.
    """

    def __init__(self, *, hidden: frozenset[str] = _TOOLS_TO_HIDE) -> None:
        super().__init__()
        self._hidden = hidden

    def wrap_model_call(self, request: Any, handler: Callable[[Any], Any]) -> Any:
        return handler(self._filtered(request))

    async def awrap_model_call(
        self, request: Any, handler: Callable[[Any], Any]
    ) -> Any:
        return await handler(self._filtered(request))

    def _filtered(self, request: Any):
        kept = [
            tool
            for tool in request.tools
            if getattr(tool, "name", getattr(tool, "__name__", "")) not in self._hidden
        ]
        if len(kept) == len(request.tools):
            return request
        return request.override(tools=kept)


class RefuseDelegationMiddleware(AgentMiddleware):
    """``task`` 호출을 전부 거절한다.

    Diagnosis SubAgent를 위한 것이다. 이 Agent는 **위임의 끝**이어야 한다 —
    여기서 또 넘기면 Triage도 Evidence 선별도 Validator도 거치지 않은 무언가가
    리포트 재료로 섞여 들고, 그 비용은 이미 예약이 끝난 예산 안에서 나간다.

    평소에는 할 일이 없다. ``restrict_harness``가 general-purpose SubAgent를
    꺼서 위임처가 없으면 ``task`` 자체가 만들어지지 않기 때문이다. 이것은 그
    등록이 키 불일치로 조용히 빠졌을 때를 위한 층이다 — Main Agent 쪽은
    ``DelegationGuardrailMiddleware``가 diagnosis 아닌 위임처를 거절해 같은
    구멍이 막혀 있는데, 이쪽에는 그에 해당하는 것이 없었다.
    """

    def wrap_tool_call(self, request: Any, handler: Callable[[Any], Any]) -> Any:
        call = getattr(request, "tool_call", None) or {}
        if call.get("name") != _TASK_TOOL_NAME:
            return handler(request)
        return ToolMessage(
            content=(
                "진단 SubAgent는 다른 Agent에게 위임할 수 없다. "
                "주어진 도구로 이 구간을 직접 처리하라."
            ),
            tool_call_id=call.get("id", ""),
            name=_TASK_TOOL_NAME,
            status="error",
        )
