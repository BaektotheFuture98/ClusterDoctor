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

import pytest

from cluster_doctor.application.port.outbound.llm_analyzer import LlmApiError

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


# --------------------------------------------------------------------------
# 저하된 실행(degraded run)
#
# analyze_logs는 분석이 실패해도 예외를 올리지 않고 오류 문자열을 돌려준다
# (tool 예외는 agent 실행 전체를 죽인다). 그래서 429로 전 구간이 실패해도
# agent는 "분석하지 못했다"는 리포트를 정상 종료로 써 내고, 트리거 서비스는
# 그것을 성공으로 보고 큐에 남은 항목을 근거로 지연 없이 다시 실행한다 —
# 실패한 실행이 상한까지 연달아 돌며 할당량만 태운다. tool이 저하를 표시하면
# analyze()가 그것을 실패로 승격해 재실행 경로를 끊는다.
# --------------------------------------------------------------------------

def _run_analyze_with_tools(make_tools_impl, agent_text: str = "분석 실패 리포트"):
    log_time = datetime(2026, 8, 27, 3, 0, 0, tzinfo=_UTC)
    kafka_receive_time = log_time + timedelta(seconds=5)

    with (
        patch(
            "cluster_doctor.infrastructure.outbound.llm.deepagent.analyzer.ChatGoogleGenerativeAI"
        ),
        patch(
            "cluster_doctor.infrastructure.outbound.llm.deepagent.analyzer.create_deep_agent"
        ) as create_deep_agent,
        patch(
            "cluster_doctor.infrastructure.outbound.llm.deepagent.analyzer.make_tools",
            side_effect=make_tools_impl,
        ),
    ):
        agent = MagicMock()
        agent.invoke.return_value = _make_agent_response(agent_text)
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
        return analyzer.analyze(log_time, kafka_receive_time)


def test_a_tool_marking_the_run_degraded_makes_analyze_fail():
    def _degrading(**kwargs):
        # analyze_logs가 실패 문자열을 돌려줄 때 하는 일과 같다.
        kwargs["run_state"]["degraded"] = True
        return []

    with pytest.raises(LlmApiError):
        _run_analyze_with_tools(_degrading)


def test_a_clean_run_still_returns_the_report():
    # 저하 표시가 없으면 지금까지처럼 리포트를 그대로 돌려줘야 한다.
    # (무조건 raise하는 구현으로는 위 테스트가 통과해 버린다.)
    def _clean(**kwargs):
        assert kwargs["run_state"] == {"degraded": False}
        return []

    assert _run_analyze_with_tools(_clean, agent_text="정상 리포트") == "정상 리포트"
