"""Main Agent(Supervisor) 조립.

무엇을 조사할지는 모델이 정하고, 이 모듈은 그 모델이 쓸 수 있는 것과 넘지
못할 선을 묶어 건넨다 — tool 묶음, 시스템 프롬프트, 구조화 출력 스키마,
파일시스템 차단, 런타임 상한.

실행 결과를 리포트로 만드는 일은 여기 없다. 그것은 ``analyzer``의 폴백
사다리가 맡는다.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime
from functools import partial

from langchain.agents.structured_output import ToolStrategy
from langchain_litellm import ChatLiteLLM
from deepagents import create_deep_agent, FilesystemPermission

from cluster_doctor.application.port.outbound.cluster_repository import ClusterRepository
from cluster_doctor.application.port.outbound.node_log_fetcher import NodeLogFetcher
from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.infrastructure.outbound.agent.common.litellm_client import complete
from cluster_doctor.infrastructure.outbound.agent.supervisor.guardrails import (
    LLM_MAX_RETRIES,
    STRUCTURED_OUTPUT_RETRY_LIMIT,
)
from cluster_doctor.infrastructure.outbound.agent.supervisor.prompt import SYSTEM_PROMPT
from cluster_doctor.infrastructure.outbound.agent.supervisor.run_state import (
    DiagnosisState,
)
from cluster_doctor.infrastructure.outbound.agent.supervisor.schema import ReportNarrative
from cluster_doctor.infrastructure.outbound.agent.supervisor.tools import make_tools
from cluster_doctor.infrastructure.outbound.agent.workflows.minute_analysis.nodes import (
    MinuteOutput,
)

_logger = logging.getLogger(__name__)

# agent는 파일을 읽거나 쓸 이유가 없다. deepagents가 기본으로 주는
# 파일시스템 tool을 통째로 막는다.
_DENY_FILESYSTEM = FilesystemPermission(
    operations=["read", "write"],
    paths=["/**"],
    mode="deny",
)


def _litellm_call(
    messages: list[dict],
    max_tokens: int,
    *,
    provider: str,
    model: str,
    api_key: str,
    response_format=None,
) -> str:
    return complete(
        messages=messages,
        provider=provider,
        model=model,
        api_key=api_key,
        max_tokens=max_tokens,
        response_format=response_format,
    )


def _make_structured_error_handler(limit: int):
    """구조화 출력 검증 실패를 몇 번까지 되돌려 볼지 정한다.

    ``handle_errors=True``(기본값)는 **횟수 제한이 없다.** 실패할 때마다 오류
    ToolMessage를 붙여 다시 시키고, deepagents의 ``recursion_limit``이 9,999라
    프레임워크도 막지 않는다. 이 저장소는 429를 최우선 제약으로 다뤄 재시도를
    일부러 0으로 둔 곳이라, 그 결정을 스키마가 우회하게 둘 수 없다.

    ``handle_errors=False``도 답이 아니다. 예외가 그대로 올라와 agent 실행이
    통째로 죽고, 그러면 코드가 모아 둔 관측값까지 함께 잃는다.

    그래서 상한을 넘으면 **평문으로 답하라고 안내한다.** 그 답은 폴백 사다리
    2단이 받아 리포트가 되므로, 재시도를 끊어도 진단은 남는다.

    스키마는 길이를 제한하지 않는다(``report_schema`` 모듈 docstring 참고 —
    한국어가 글자 단위로 끊겨 ``follower_check``가 갈린 뒤로 잘라내기를 없앴다).
    그러므로 여기까지 오는 것은 타입이 어긋난 경우뿐이고, 실측에서는 아직 본
    적이 없다.
    """
    state = {"count": 0}

    def handle(exc: Exception) -> str:
        state["count"] += 1
        _logger.warning(
            "구조화 리포트 검증 실패 %d/%d: %s", state["count"], limit, exc
        )
        if state["count"] >= limit:
            return (
                "구조화 리포트 제출이 계속 실패했다. 더 시도하지 말고 "
                "지금까지의 분석을 평문으로 작성해 답하라."
            )
        return f"리포트 형식이 스키마와 맞지 않는다: {exc}. 형식을 고쳐 다시 제출하라."

    return handle


def build_supervisor(
    *,
    provider: str,
    model: str,
    api_key: str,
    cluster: ClusterRepository,
    fetch_logs: Callable[[TimeRange], list[LogEntry]],
    drain_pending: Callable[[], list[LogEntry]],
    node_log_fetcher: NodeLogFetcher,
    fetch_node_logs: Callable[..., list],
    log_time: datetime,
    kafka_receive_time: datetime,
):
    """Main Agent와 그 실행 상태를 만들어 돌려준다. ``(agent, state)``.

    ``state``를 함께 돌려주는 이유: tool은 실패를 예외가 아니라 문자열로
    돌려주므로(예외는 agent 실행 전체를 죽인다) 무엇을 보았고 무엇을 놓쳤는지가
    반환값만으로는 호출자에게 닿지 않는다.
    """
    _bound = partial(
        _litellm_call,
        provider=provider,
        model=model,
        api_key=api_key,
    )
    call_llm = _bound
    call_llm_minute = partial(_bound, response_format=MinuteOutput)

    llm = ChatLiteLLM(
        # litellm의 모델 문자열은 "<provider>/<model>" 형태다.
        # litellm_client._PROVIDER_PREFIX와 같은 규칙을 쓴다.
        model=f"{provider}/{model}",
        api_key=api_key,
        # 재시도하지 않는다. 이 경로의 실패는 대부분 429이고, 그것은 분당
        # 입력 토큰 한도 초과가 원인이다. 같은 프롬프트를 다시 보내면
        # 실패가 보장된 채 소비만 배로 늘어난다 —
        # complete()의 num_retries=0과 같은 이유다(실측 513,122 토큰 →
        # 재시도 포함 2,052,488 토큰, 한도 250,000의 821%).
        #
        # 오케스트레이터는 tool 루프라 재시도가 루프 전체로 증폭되고,
        # 트리거 서비스가 큐 잔여 시 10초 간격으로 최대 4회 연속
        # 실행하므로(_MAX_CONSECUTIVE_RETRIGGERS=3) 증폭이 한 번 더 곱해진다.
        #
        # 예전 ChatGoogleGenerativeAI에서는 1이었다. 그쪽은 0을 "SDK
        # 기본값을 쓰라"(5회)로 해석하는 특수값이었기 때문이다.
        # ChatLiteLLM에는 그 해석이 없으므로 0이 곧 재시도 없음이다.
        max_retries=LLM_MAX_RETRIES,
    )

    # tool은 실패를 예외가 아니라 문자열로 돌려준다(예외는 agent 실행
    # 전체를 죽인다). 그 사실을 여기로 실어 나르는 통로다.
    state = DiagnosisState(log_time=log_time, kafka_receive_time=kafka_receive_time)
    tools = make_tools(
        cluster=cluster,
        fetch_logs=fetch_logs,
        drain_pending=drain_pending,
        call_llm=call_llm,
        call_llm_minute=call_llm_minute,
        node_log_fetcher=node_log_fetcher,
        fetch_node_logs=fetch_node_logs,
        state=state,
    )

    # 구조화 출력을 건다. langchain은 이 모델(ChatLiteLLM + nvidia_nim/gemini)에
    # 대해 native json_schema를 쓰지 않고 ToolStrategy로 떨어진다 — 스키마가
    # **평범한 tool 하나로** 바인딩된다는 뜻이다(실측 확인).
    #
    # 그래서 두 가지가 따라온다. 첫째, 모델이 그 tool을 부르지 않고 평문으로
    # 끝낼 수 있다. 둘째, 결과가 result["structured_response"]로 가고
    # messages[-1]은 ToolMessage가 된다. 아래 폴백 사다리가 둘 다 받는다.
    #
    # ToolStrategy를 명시적으로 만드는 이유는 handle_errors 때문이다. 기본값은
    # 무제한 재시도이고, 이 저장소의 "재시도하지 않는다" 결정과 정면으로
    # 어긋난다.
    agent = create_deep_agent(
        model=llm,
        tools=tools,
        system_prompt=SYSTEM_PROMPT,
        permissions=[_DENY_FILESYSTEM],
        response_format=ToolStrategy(
            schema=ReportNarrative,
            handle_errors=_make_structured_error_handler(
                STRUCTURED_OUTPUT_RETRY_LIMIT
            ),
        ),
    )

    return agent, state
