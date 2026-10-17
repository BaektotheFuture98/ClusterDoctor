"""Main DeepAgent 조립.

도구·위임 대상·middleware와 파일 접근 권한을 설정한다.
분석 범위와 예산의 승인은 Guardrail middleware와 도구가 실행 시 검사한다.
"""

from __future__ import annotations

from collections.abc import Sequence

from deepagents import CompiledSubAgent, create_deep_agent
from langchain.agents.middleware import AgentMiddleware
from langchain_core.language_models import BaseChatModel
from langchain_core.runnables import Runnable
from langchain_core.tools import BaseTool

from cluster_doctor.incident_orchestrator_agent.agent.prompts.system_prompt import SYSTEM_PROMPT
from cluster_doctor.incident_analysis_agent.agent.runtime.harness import (
    DENY_ALL_FILESYSTEM,
    HideHarnessToolsMiddleware,
)
from cluster_doctor.incident_analysis_agent.agent.runtime.pseudonym import (
    PseudonymizeMiddleware,
)
from cluster_doctor.incident_analysis_agent.agent.runtime.llm_call_log import AgentRoleLogMiddleware
from cluster_doctor.incident_analysis_agent.agent.runtime.rate_limit_retry import (
    RateLimitRetryMiddleware,
)
from cluster_doctor.incident_orchestrator_agent.agent.state import MainAgentState
from cluster_doctor.incident_orchestrator_agent.agent.middleware import (
    AnalysisTaskDescriptionMiddleware,
)


def build_main_agent(
    *,
    model: BaseChatModel,
    tools: Sequence[BaseTool],
    middleware: Sequence[AgentMiddleware],
    analysis_subagent: CompiledSubAgent,
) -> Runnable:
    """도구·Guardrail·SubAgent를 하나의 DeepAgent로 묶는다.

    모델·업무 도구·위임 대상을 주입받고 공통 middleware를 추가한다.
    Incident에 묶인 위임 대상은 AnalysisSubagentAdapter.bind()가 만든다.
    """
    return create_deep_agent(
        model,
        tools,
        system_prompt=SYSTEM_PROMPT,
        # 도구를 추가하는 미들웨어 뒤에서 harness 기본 도구를 걸러낸다.
        middleware=[
            *middleware,
            AnalysisTaskDescriptionMiddleware(),
            HideHarnessToolsMiddleware(),
            AgentRoleLogMiddleware("main"),
            RateLimitRetryMiddleware(),
            PseudonymizeMiddleware(),
        ],
        # 위임처는 analysis 하나뿐이다.
        subagents=[analysis_subagent],
        # 구조화된 값이 SubAgent에 닿는 통로는 이 state뿐이다. task 도구는
        # 자유 텍스트 description밖에 넘기지 못하므로, 승인된 구간은 문장이
        # 아니라 여기에 실려 건너간다(agent/state.py를 볼 것).
        state_schema=MainAgentState,
        # 심층 방어. ``restrict_harness``가 파일 도구를 모델의 목록에서 지우지만
        # 그것은 **보이지 않게 하는 것**이고, 도구 자체는 ToolNode에 묶인 채
        # 남는다. 모델이 목록에 없는 이름을 지어내 호출하면 그대로 실행된다.
        # 이쪽은 deepagents가 "security guarantee"라고 부르는 집행 층이라,
        # 호출이 뚫고 들어와도 여기서 막힌다.
        permissions=DENY_ALL_FILESYSTEM,
    )
