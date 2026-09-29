"""Main DeepAgent 조립.

**여기서 정책을 강제하지 않는다.** 상한을 지키게 하는 것은 Guardrail
middleware와 도구 코드이고, 이 모듈이 하는 일은 그 부품들을 하나의 그래프로
묶는 것뿐이다. 프롬프트가 하는 일은 강제가 아니라 안내다 — 모델이 거절당한
요청을 되풀이해 사이클만 태우는 일을 줄이는 것.
"""

from __future__ import annotations

from collections.abc import Sequence

from deepagents import CompiledSubAgent, create_deep_agent
from langchain.agents.middleware import AgentMiddleware
from langchain_core.language_models import BaseChatModel
from langchain_core.runnables import Runnable
from langchain_core.tools import BaseTool

from cluster_doctor.incident_orchestrator_agent.agent.prompts.system_prompt import SYSTEM_PROMPT
from cluster_doctor.incident_orchestrator_agent.agent.runtime.harness import (
    DENY_ALL_FILESYSTEM,
    HideHarnessToolsMiddleware,
    restrict_harness,
)
from cluster_doctor.incident_orchestrator_agent.model.state.main_agent_state import MainAgentState


def build_main_agent(
    *,
    model: BaseChatModel,
    tools: Sequence[BaseTool],
    middleware: Sequence[AgentMiddleware],
    analysis_subagent: CompiledSubAgent,
) -> Runnable:
    """도구·Guardrail·SubAgent를 하나의 DeepAgent로 묶는다.

    부품을 만들지 않고 받기만 하는 것이 요점이다. 도구도 Guardrail도
    SubAgent도 각자 자기 모듈에서 조립되고, 여기서는 배선만 한다 — 이 함수가
    부품을 만들기 시작하면 테스트가 진짜 LLM과 진짜 ClickHouse를 필요로 하게
    된다.
    """
    restrict_harness(model)

    return create_deep_agent(
        model,
        tools,
        system_prompt=SYSTEM_PROMPT,
        # harness 도구 감추기를 **맨 뒤에** 둔다. 앞쪽 미들웨어가 도구를
        # 끼워 넣은 뒤에 걸러야 그 도구까지 걸린다.
        middleware=[*middleware, HideHarnessToolsMiddleware()],
        # 위임처는 analysis 하나뿐이다.
        subagents=[analysis_subagent],
        # 구조화된 값이 SubAgent에 닿는 통로는 이 state뿐이다. task 도구는
        # 자유 텍스트 description밖에 넘기지 못하므로, 승인된 구간은 문장이
        # 아니라 여기에 실려 건너간다(model/state/main_agent_state.py를 볼 것).
        state_schema=MainAgentState,
        # 심층 방어. ``restrict_harness``가 파일 도구를 모델의 목록에서 지우지만
        # 그것은 **보이지 않게 하는 것**이고, 도구 자체는 ToolNode에 묶인 채
        # 남는다. 모델이 목록에 없는 이름을 지어내 호출하면 그대로 실행된다.
        # 이쪽은 deepagents가 "security guarantee"라고 부르는 집행 층이라,
        # 호출이 뚫고 들어와도 여기서 막힌다.
        permissions=DENY_ALL_FILESYSTEM,
    )
