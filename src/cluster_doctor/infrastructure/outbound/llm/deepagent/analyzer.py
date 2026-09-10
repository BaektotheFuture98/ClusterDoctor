"""deepagents 기반 클러스터 진단 어댑터.

LlmAnalyzer 포트 구현.
- trigger_time만 받아 agent가 직접 분석 구간과 순서를 결정한다.
- fetch_logs / drain_pending은 생성자 주입 → tool 클로저로 전달.
"""
from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from functools import partial

_KST = timezone(timedelta(hours=9))

from langchain_litellm import ChatLiteLLM
from deepagents import create_deep_agent, FilesystemPermission

from cluster_doctor.application.port.outbound.cluster_repository import ClusterRepository
from cluster_doctor.application.port.outbound.llm_analyzer import (
    LlmAnalyzer,
    LlmApiError,
    LlmResponseError,
)
from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.infrastructure.outbound.llm.deepagent.tools import make_tools
from cluster_doctor.infrastructure.outbound.ssh.node_log_fetcher import NodeLogFetcher
from cluster_doctor.infrastructure.outbound.llm.deepagent.prompts import SYSTEM_PROMPT
from cluster_doctor.infrastructure.outbound.llm.langgraph.nodes import MinuteOutput
from cluster_doctor.infrastructure.outbound.llm.litellm_client import (
    complete,
    require_supported_provider,
)

_DENY_FILESYSTEM = FilesystemPermission(
    operations=["read", "write"],
    paths=["/**"],
    mode="deny",
)


class DeepAgentAnalyzer(LlmAnalyzer):
    """deepagents 오케스트레이터 + ES tool + LangGraph 분석을 결합한 어댑터.

    SlowlogTriggerService가 asyncio.to_thread()로 감싸 호출하므로
    analyze()는 blocking sync로 구현한다.
    """

    def __init__(
        self,
        api_key: str,
        default_model: str,
        cluster: ClusterRepository,
        fetch_logs: Callable[[TimeRange], list[LogEntry]],
        drain_pending: Callable[[], list[LogEntry]],
        node_log_fetcher: NodeLogFetcher,
        provider: str = "gemini",
    ) -> None:
        # 생성자에서 검증한다. 잘못된 provider를 첫 호출까지 끌고 가면
        # 진단 요청 한 건을 통째로 날린 뒤에야 오타를 알게 된다.
        self._provider = require_supported_provider(provider)
        self._api_key = api_key
        self._default_model = default_model
        self._cluster = cluster
        self._fetch_logs = fetch_logs
        self._drain_pending = drain_pending
        self._node_log_fetcher = node_log_fetcher

    def analyze(self, log_time: datetime, kafka_receive_time: datetime) -> str:
        _bound = partial(
            _litellm_call,
            provider=self._provider,
            model=self._default_model,
            api_key=self._api_key,
        )
        call_llm = _bound
        call_llm_minute = partial(_bound, response_format=MinuteOutput)

        llm = ChatLiteLLM(
            # litellm의 모델 문자열은 "<provider>/<model>" 형태다.
            # litellm_client._PROVIDER_PREFIX와 같은 규칙을 쓴다.
            model=f"{self._provider}/{self._default_model}",
            api_key=self._api_key,
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
            max_retries=0,
        )

        # tool은 실패를 예외가 아니라 문자열로 돌려준다(예외는 agent 실행
        # 전체를 죽인다). 그 사실을 여기로 실어 나르는 통로다.
        run_state = {"degraded": False}
        tools = make_tools(
            cluster=self._cluster,
            fetch_logs=self._fetch_logs,
            drain_pending=self._drain_pending,
            call_llm=call_llm,
            call_llm_minute=call_llm_minute,
            node_log_fetcher=self._node_log_fetcher,
            run_state=run_state,
        )

        agent = create_deep_agent(
            model=llm,
            tools=tools,
            system_prompt=SYSTEM_PROMPT,
            permissions=[_DENY_FILESYSTEM],
        )

        log_time_kst = log_time.astimezone(_KST)
        kafka_time_kst = kafka_receive_time.astimezone(_KST)
        result = agent.invoke({
            "messages": [(
                "user",
                (
                    f"slowlog_timestamp: {log_time_kst.strftime('%Y-%m-%d %H:%M:%S')} KST (slowlog 자체 기재 시각)\n"
                    f"kafka_receive_time: {kafka_time_kst.strftime('%Y-%m-%d %H:%M:%S')} KST (Kafka 수신 시각)\n"
                    "모든 시각은 KST 기준이다. analyze_logs 호출 시 start_iso/end_iso도 KST 기준으로 입력하라.\n"
                    "두 시각을 참고해 적절한 trigger_time을 판단하고 ES slowlog 원인을 분석하라."
                ),
            )]
        })
        content = result["messages"][-1].content
        if isinstance(content, list):
            content = "\n".join(
                block["text"]
                for block in content
                if isinstance(block, dict) and block.get("type") == "text"
            )
        if not content:
            raise LlmResponseError("agent가 빈 응답을 반환했습니다.")
        if run_state["degraded"]:
            # 분석이 실패한 채로 리포트가 작성됐다. agent는 정상 종료했지만
            # 진단은 이뤄지지 않았다. 이것을 성공으로 돌려주면 트리거 서비스가
            # 큐에 남은 항목을 보고 곧바로 같은 실행을 다시 건다 — 429로
            # 실패한 실행을 지연 없이 3번 더 반복하며 할당량만 태운다.
            raise LlmApiError(f"분석이 실패한 채 리포트가 작성되었습니다: {content[:200]}")
        return content


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
