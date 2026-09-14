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
from cluster_doctor.infrastructure.outbound.llm.deepagent.report_schema import (
    ReportNarrative,
)

_UTC = timezone.utc


def _make_agent_response(text: str = "리포트", *, structured=True):
    """agent.invoke의 반환을 흉내낸다.

    ``structured=True``가 정상 경로다 — ToolStrategy가 스키마를 tool로
    바인딩하고, 모델이 그것을 부르면 결과가 structured_response로 온다.
    ``False``는 모델이 그 tool을 부르지 않고 평문으로 끝낸 경우이며,
    폴백 사다리 2단이 받는 자리다.
    """
    message = MagicMock()
    message.content = text
    message.type = "ai"
    # 실제 AIMessage는 tool_call이 없으면 빈 리스트다. MagicMock에 맡기면
    # 자동 생성된 속성이 늘 truthy라, "아직 일하는 중인 메시지"를 걸러내는
    # _last_model_text의 판정이 모든 메시지에 걸린다.
    message.tool_calls = []
    narrative = ReportNarrative(headline=text) if structured else None
    return {"messages": [message], "structured_response": narrative}


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
            fetch_node_logs=MagicMock(return_value=[]),
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
            fetch_node_logs=MagicMock(return_value=[]),
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

def _run_analyze_with_tools(
    make_tools_impl, agent_text: str = "분석 실패 리포트", *, structured: bool = True
):
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
        agent.invoke.return_value = _make_agent_response(
            agent_text, structured=structured
        )
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
            fetch_node_logs=MagicMock(return_value=[]),
        )
        return analyzer.analyze(log_time, kafka_receive_time)


def test_a_degraded_run_still_delivers_the_report():
    # 예전에는 여기서 LlmApiError를 던졌고, 그러면 _run_agent이 notify를
    # 건너뛰어 리포트가 통째로 사라졌다 — 운영자는 logs/app.log를 뒤져야
    # 실패를 알 수 있었다. 이제 본문은 전달하고 재트리거만 막는다.
    def _degrading(**kwargs):
        # analyze_logs가 실패 문자열을 돌려줄 때 하는 일과 같다.
        kwargs["run_state"]["degraded"] = True
        return []

    result = _run_analyze_with_tools(_degrading, agent_text="분석 실패 리포트")

    assert result.report.narrative.headline == "분석 실패 리포트"
    assert result.analysis_failed is True


def test_supplementary_gaps_do_not_fail_the_run():
    # 보조 조사(노드 로그 SSH 수집)가 실패해도 4단계 분석 결과는 온전하다.
    # 재트리거를 막지 않고, 빠진 사실만 결과에 실어 보낸다.
    def _with_gap(**kwargs):
        kwargs["run_state"]["gaps"].append("es-data-02 노드 로그 SSH 수집 실패")
        return []

    result = _run_analyze_with_tools(_with_gap, agent_text="정상 리포트")

    assert result.report.narrative.headline == "정상 리포트"
    assert result.analysis_failed is False
    assert result.gaps == ("es-data-02 노드 로그 SSH 수집 실패",)


def test_a_clean_run_still_returns_the_report():
    def _clean(**kwargs):
        assert kwargs["run_state"] == {"degraded": False, "gaps": []}
        return []

    result = _run_analyze_with_tools(_clean, agent_text="정상 리포트")

    assert result.report.narrative.headline == "정상 리포트"
    assert result.report.narrative_text == ""
    assert result.analysis_failed is False
    assert result.gaps == ()


def _seed_observation(run_state) -> None:
    """관측값이 하나라도 있는 상태를 만든다.

    ``analyze_logs``가 한 번이라도 성공한 실행과 같은 모양이다. 2단 폴백의
    판정이 관측값 유무로 갈리므로 두 갈래를 나눠 검증하려면 이 씨앗이 필요하다.
    """
    from datetime import datetime as _dt

    from cluster_doctor.domain.model.diagnosis_report import TimelineRow

    minute = _dt(2026, 8, 27, 3, 0, tzinfo=timezone.utc)
    run_state.setdefault("observations", {})["timeline"] = {
        minute: TimelineRow(minute=minute, counts={"slowlog": 3})
    }


def test_구조화_실패는_평문으로_떨어지되_진단을_버리지_않는다():
    """폴백 사다리 2단 — 관측값이 온전한 경우.

    ToolStrategy는 스키마를 평범한 tool 하나로 바인딩할 뿐이라, 모델이
    그것을 부르지 않고 평문으로 끝내는 갈래가 열려 있다. 그때도 관측값은
    온전하므로 분석 실패가 아니다 — 빠진 사실만 gaps로 남긴다.
    """

    def _with_observation(**kwargs):
        _seed_observation(kwargs["run_state"])
        return []

    result = _run_analyze_with_tools(
        _with_observation, agent_text="평문 리포트", structured=False
    )

    assert result.report.narrative is None
    assert result.report.narrative_text == "평문 리포트"
    assert result.analysis_failed is False
    assert any("구조화 리포트를 제출하지 않아" in gap for gap in result.gaps)


def test_근거_없는_평문은_정상_진단으로_나가지_않는다():
    """폴백 사다리 2단 — 관측값이 하나도 없는 경우.

    관측값이 비었다는 것은 analyze_logs가 한 번도 성공하지 않았다는 뜻이다.
    429 직후 모델이 "로그를 확인할 수 없습니다" 한 줄로 끝내면 이 갈래에
    떨어지는데, 예전에는 그것이 analysis_failed=False로 나가 재트리거까지
    허용됐다 — 근거가 하나도 없는 판단이 정상 진단으로 보였다.

    평문은 그대로 싣는다. 예외를 올리면 "리포트는 항상 전달된다"가 깨진다.
    """
    result = _run_analyze_with_tools(
        lambda **kwargs: [], agent_text="로그를 확인할 수 없습니다", structured=False
    )

    assert result.report.narrative_text == "로그를 확인할 수 없습니다"
    assert result.report.observations.is_empty()
    assert result.analysis_failed is True


def test_구조화도_평문도_없으면_관측값만으로_리포트를_만든다():
    """폴백 사다리 3단과 4단.

    예전에는 빈 응답이 곧 LlmResponseError였다. 그것은 "전달할 것이 없다"가
    참이었을 때의 판단이고, 이제는 코드가 모은 관측값이 있다. 관측값마저
    비었을 때만 예외를 올린다.
    """
    from datetime import datetime as _dt

    from cluster_doctor.application.port.outbound.llm_analyzer import (
        LlmResponseError,
    )
    from cluster_doctor.domain.model.diagnosis_report import TimelineRow

    def _with_observations(**kwargs):
        kwargs["run_state"]["observations"] = {
            "timeline": {
                _dt(2026, 9, 10, 15, 27, tzinfo=_UTC): TimelineRow(
                    minute=_dt(2026, 9, 10, 15, 27, tzinfo=_UTC),
                    counts={"es_query_log": 264},
                )
            },
            "nodes": {},
            "master_logs": {},
            "master_log_total": 0,
            "health": [],
            "candidates": {},
            "wait_seconds": 0.0,
            "wait_cap_reached": False,
        }
        return []

    # 3단 — 관측값이 있으면 리포트가 나온다
    result = _run_analyze_with_tools(_with_observations, agent_text="", structured=False)
    assert result.report.observations.timeline
    assert result.analysis_failed is True

    # 4단 — 관측값도 없으면 그때만 예외
    with pytest.raises(LlmResponseError):
        _run_analyze_with_tools(lambda **kwargs: [], agent_text="", structured=False)


# --------------------------------------------------------------------------
# _last_model_text — 진행 안내문을 리포트로 집지 않는다
#
# gemini 계열은 tool_call과 안내 문장을 한 AIMessage에 함께 싣는 일이 흔하다.
# "본문이 있는 마지막 메시지"를 찾아 거슬러 올라가면, 마지막 턴이 조용히
# 끝났을 때 중간 안내문이 리포트가 되어 나간다.
# --------------------------------------------------------------------------

def _msg(text: str, *, kind: str = "ai", tool_calls=()):
    message = MagicMock()
    message.content = text
    message.type = kind
    message.tool_calls = list(tool_calls)
    return message


def test_마지막_모델_메시지의_본문을_쓴다():
    from cluster_doctor.infrastructure.outbound.llm.deepagent.analyzer import (
        _last_model_text,
    )

    messages = [_msg("먼저 구간을 보겠습니다", tool_calls=[{"name": "analyze_logs"}]),
                _msg("도구 결과", kind="tool"),
                _msg("최종 리포트입니다")]

    assert _last_model_text(messages) == "최종 리포트입니다"


def test_구조화_tool_안내문은_리포트가_되지_않는다():
    from cluster_doctor.infrastructure.outbound.llm.deepagent.analyzer import (
        _last_model_text,
    )

    # ToolStrategy가 스키마를 tool로 바인딩하므로 마지막 메시지는
    # ToolMessage("Returning structured response: …")가 된다(실측).
    messages = [_msg("평문 리포트"),
                _msg("Returning structured response: ...", kind="tool")]

    assert _last_model_text(messages) == "평문 리포트"


def test_아직_일하는_중인_메시지는_리포트로_집지_않는다():
    from cluster_doctor.infrastructure.outbound.llm.deepagent.analyzer import (
        _last_model_text,
    )

    # recursion_limit 도달이나 중간 중단으로 끝난 모양. 안내문을 집으면
    # 운영자가 "먼저 …구간을 보겠습니다" 한 줄짜리 진단을 받는다.
    messages = [_msg("이전 턴의 요약"),
                _msg("도구 결과", kind="tool"),
                _msg("이제 16:09 구간을 보겠습니다",
                     tool_calls=[{"name": "analyze_logs"}])]

    assert _last_model_text(messages) == ""


def test_사람과_시스템_메시지는_모델이_쓴_것이_아니다():
    from cluster_doctor.infrastructure.outbound.llm.deepagent.analyzer import (
        _last_model_text,
    )

    messages = [_msg("모델이 쓴 것"),
                _msg("지시문", kind="system"),
                _msg("사용자 입력", kind="human")]

    assert _last_model_text(messages) == "모델이 쓴 것"


def test_메시지가_없으면_빈_문자열이다():
    from cluster_doctor.infrastructure.outbound.llm.deepagent.analyzer import (
        _last_model_text,
    )

    assert _last_model_text([]) == ""
    assert _last_model_text(None) == ""
