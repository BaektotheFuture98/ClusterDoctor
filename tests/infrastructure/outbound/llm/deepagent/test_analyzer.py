"""DeepAgentAnalyzer.analyze()가 오케스트레이터 LLM에 넘기는 인자를 고정한다.

두 가지를 못 박는다.

1. ``max_retries=0`` — 재시도를 끈다. 이 경로의 실패는 대부분 429(분당 입력
   토큰 한도 초과)이고, 같은 프롬프트를 다시 보내면 실패가 보장된 채 소비만
   배로 늘어난다(``complete()``의 ``num_retries=0``과 같은 이유). 오케스트레이터는
   tool 루프라 재시도가 루프 전체로 증폭되고, Kafka 트리거가 큐 잔여 시 10초
   간격으로 최대 4회 연속 실행하므로(``_MAX_CONSECUTIVE_RETRIGGERS=3``) 증폭이
   한 번 더 곱해진다.

   주의: 예전에는 ``max_retries=1``이었다. ``langchain_google_genai``에서 0은
   "Google SDK 기본값을 쓰라"(5회 재시도)로 해석되는 특수값이었기 때문이다.
   ``ChatLiteLLM``에는 그 해석이 없으므로 0이 곧 재시도 없음이다.

2. provider와 모델이 코드에 박히지 않고 주입된 값을 쓴다는 것. 예전에는
   Gemini 전용 ``ChatGoogleGenerativeAI``였고 provider가 하드코딩돼 있어,
   ``.env``에 무엇을 적어도 Gemini로 갔다.

둘 다 예외 없이 조용히 어긋나므로 인자 자체를 고정해 두지 않으면 아무도
못 알아챈다.
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


def _orchestrator_kwargs(provider="gemini", default_model="gemini-3.5-flash-lite"):
    """analyze()를 한 번 돌리고 오케스트레이터 LLM에 넘어간 인자를 돌려준다."""
    log_time = datetime(2026, 8, 27, 3, 0, 0, tzinfo=_UTC)
    kafka_receive_time = log_time + timedelta(seconds=5)

    with (
        patch(
            "cluster_doctor.infrastructure.outbound.llm.deepagent.analyzer.ChatLiteLLM"
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
            provider=provider,
            api_key="test-key",
            default_model=default_model,
            cluster=MagicMock(),
            fetch_logs=MagicMock(),
            drain_pending=MagicMock(),
            node_log_fetcher=MagicMock(),
        )

        analyzer.analyze(log_time, kafka_receive_time)

    return chat_llm_cls.call_args.kwargs


def test_orchestrator_does_not_retry():
    # 429는 분당 입력 토큰 한도 초과로 난다. 같은 프롬프트를 다시 보내면
    # 실패가 보장된 채 소비만 배로 늘어난다. tool 루프라 재시도가 루프
    # 전체로 증폭되고, Kafka 재트리거가 그것을 또 곱한다.
    assert _orchestrator_kwargs()["max_retries"] == 0


def test_orchestrator_uses_the_selected_provider_and_model():
    # litellm의 모델 문자열은 "<provider>/<model>"이다. provider가 코드에
    # 박혀 있으면 .env에 무엇을 적어도 그쪽으로 가지 않는다.
    kwargs = _orchestrator_kwargs(
        provider="nvidia_nim", default_model="google/gemma-4-31b-it"
    )
    assert kwargs["model"] == "nvidia_nim/google/gemma-4-31b-it"


def test_orchestrator_api_key_is_not_folded_into_the_model_string():
    # 키가 모델 문자열에 실리면 provider 에러 메시지·로그로 새어 나간다.
    assert "test-key" not in _orchestrator_kwargs()["model"]


def test_unsupported_provider_is_rejected_at_construction():
    # 잘못된 provider를 첫 호출까지 끌고 가면 진단 요청 한 건을 통째로
    # 날린 뒤에야 오타를 알게 된다.
    from cluster_doctor.infrastructure.outbound.llm.deepagent.analyzer import (
        DeepAgentAnalyzer,
    )

    with pytest.raises(ValueError, match="nvidia_nim"):
        DeepAgentAnalyzer(
            provider="nvidia",
            api_key="test-key",
            default_model="google/gemma-4-31b-it",
            cluster=MagicMock(),
            fetch_logs=MagicMock(),
            drain_pending=MagicMock(),
            node_log_fetcher=MagicMock(),
        )


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
            "cluster_doctor.infrastructure.outbound.llm.deepagent.analyzer.ChatLiteLLM"
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
            node_log_fetcher=MagicMock(),
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
