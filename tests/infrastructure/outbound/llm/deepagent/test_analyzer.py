"""DeepAgentAnalyzer.analyze()가 ChatGoogleGenerativeAI에 넘기는 인자를 고정한다.

여기서 검증하는 것은 ``max_retries`` 값 하나다. langchain_google_genai가 명시하듯
``max_retries=0``은 "Google SDK 기본값을 쓰라"(5회 재시도)로 해석되고, 재시도를
정말로 끄려면 1을 줘야 한다(초기 요청 1회만 보내고 재시도 없음). 0과 1이 뒤집히면
429(분당 입력 토큰 한도 초과)가 재시도로 계속 덧나 한도를 더 태우는데, 예외 없이
조용히 벌어져서 인자 자체를 못 박아두지 않으면 아무도 못 알아챈다.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

_UTC = timezone.utc


def _make_agent_response(text: str = "리포트"):
    message = MagicMock()
    message.content = text
    return {"messages": [message]}


def test_max_retries_is_1_not_0():
    log_time = datetime(2026, 8, 27, 3, 0, 0, tzinfo=_UTC)
    kafka_receive_time = log_time + timedelta(seconds=5)

    with (
        patch(
            "cluster_doctor.infrastructure.outbound.llm.deepagent.analyzer.ChatGoogleGenerativeAI"
        ) as chat_llm_cls,
        patch(
            "cluster_doctor.infrastructure.outbound.llm.deepagent.analyzer.create_deep_agent"
        ) as create_deep_agent,
    ):
        agent = MagicMock()
        agent.invoke.return_value = _make_agent_response()
        create_deep_agent.return_value = agent

        from cluster_doctor.infrastructure.outbound.llm.deepagent.analyzer import (
            DeepAgentAnalyzer,
        )

        analyzer = DeepAgentAnalyzer(
            api_key="test-key",
            default_model="gemini-2.5-flash",
            cluster=MagicMock(),
            fetch_logs=MagicMock(),
            drain_pending=MagicMock(),
        )

        analyzer.analyze(log_time, kafka_receive_time)

    # 0이 아니라 1이어야 재시도가 꺼진다. 0은 SDK가 기본값(5회 재시도)으로
    # 해석하는 특수값이라 429 재시도를 막으려던 의도와 정반대로 동작한다.
    assert chat_llm_cls.call_args.kwargs["max_retries"] == 1
