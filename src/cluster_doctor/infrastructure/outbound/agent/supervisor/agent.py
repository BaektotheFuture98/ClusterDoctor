"""Supervisor Agent 어댑터.

한 사이클에 **구조화 호출 한 번**을 한다. tool loop가 아니다.

tool loop 오케스트레이터를 쓰지 않는 이유는 Supervisor가 결정할 것이
``prompt.py``의 ``<decision_output>`` 다섯 칸뿐이기 때문이다. 호출할 tool이
하나(로그 분석 위임)라면 그 호출은 tool이 아니라 루프이고, 루프는
``IncidentOrchestrator``가 갖는다 — 모델에게 루프를 맡기면 상한이 프롬프트
문장이 되고, 프롬프트 문장은 강제가 아니다.

Context는 사이클마다 새로 만든다. 이전 사이클의 대화를 이어 붙이지 않는다 —
다음 판단에 필요한 것은 전부 ``IncidentState``에 있고, 대화를 이어 붙이면
Context가 분석 횟수만큼 불어난다.
"""

from __future__ import annotations

import logging
from functools import partial

from cluster_doctor.application.exception import LlmApiError, LlmResponseError
from cluster_doctor.application.service.guardrails import (
    MAX_ANALYSIS_WINDOW_MINUTES,
    MAX_ANALYZED_MINUTES,
    remaining_minutes,
)
from cluster_doctor.domain.model.incident import Incident
from cluster_doctor.domain.model.incident_state import IncidentState
from cluster_doctor.domain.model.log_analysis import LogAnalysisResponse
from cluster_doctor.domain.model.supervisor_decision import (
    SupervisorAction,
    SupervisorDecision,
)
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.infrastructure.outbound.agent.common.litellm_client import complete
from cluster_doctor.infrastructure.outbound.agent.supervisor.context import (
    build_decision_prompt,
)
from cluster_doctor.infrastructure.outbound.agent.supervisor.prompt import SYSTEM_PROMPT
from cluster_doctor.infrastructure.outbound.agent.supervisor.schema import DecisionOutput

_logger = logging.getLogger(__name__)

# 판단 하나는 짧다. 리포트를 쓰는 것이 아니라 행동 하나와 그 이유를 쓰는 것이다.
_DECISION_MAX_TOKENS = 2048


class SupervisorAgentAdapter:
    """``SupervisorAgent`` 포트의 구현."""

    def __init__(self, *, provider: str, model: str, api_key: str) -> None:
        self._call_llm = partial(
            _decide_call, provider=provider, model=model, api_key=api_key
        )

    def decide(
        self,
        incident: Incident,
        state: IncidentState,
        *,
        last_response: LogAnalysisResponse | None = None,
        candidate_windows: tuple[TimeRange, ...] = (),
    ) -> SupervisorDecision:
        """다음 행동 하나를 고른다.

        **실패하면 종료를 고른다.** 모델이 죽었다고 리포트까지 잃을 이유가
        없고, 그때까지 모은 Evidence와 관측값은 그대로 전달된다. 반대쪽으로
        기울면(실패 시 재시도) 429가 난 상황에서 호출을 더 쌓게 된다.
        """
        prompt = build_decision_prompt(
            incident,
            state,
            last_response=last_response,
            candidate_windows=candidate_windows,
            budget_note=_budget_note(state),
        )
        try:
            text = self._call_llm(
                [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                _DECISION_MAX_TOKENS,
            )
        except (LlmApiError, LlmResponseError) as exc:
            _logger.error("[supervisor] 판단 호출 실패: %s", exc)
            return _fallback(f"Supervisor 판단 호출이 실패했다: {exc}")

        try:
            decision = DecisionOutput.model_validate_json(text).to_domain()
        except Exception as exc:
            _logger.warning("[supervisor] 판단 응답을 읽지 못했다: %s", exc)
            decision = None

        if decision is None:
            return _fallback("Supervisor가 읽을 수 있는 판단을 내놓지 못했다.")

        _logger.info(
            "[supervisor] decision=%s window=%s reason=%s",
            decision.action,
            (
                f"{decision.analysis_window.start:%H:%M}~{decision.analysis_window.end:%H:%M}"
                if decision.analysis_window
                else "-"
            ),
            decision.reason,
        )
        return decision


def _budget_note(state: IncidentState) -> str:
    """남은 예산을 **분으로** 알려 준다.

    호출 수가 아니라 분인 이유는 그것이 실제 비용 단위이기 때문이다. 모델이
    예산을 계획에 쓰려면 "몇 번 더 부를 수 있는가"가 아니라 "몇 분을 더 볼 수
    있는가"를 알아야 한다 — 1분 창과 10분 창의 값이 다르다.
    """
    return (
        f"남은 분석 예산: {remaining_minutes(state)}분 "
        f"(상한 {MAX_ANALYZED_MINUTES}분, 지금까지 {state.analyzed_minutes}분 분석). "
        f"분석 창은 한 번에 최대 {MAX_ANALYSIS_WINDOW_MINUTES}분이다. "
        "남은 예산보다 긴 구간을 요청하면 런타임이 앞쪽만 잘라 분석한다. "
        "이미 분석한 구간과 겹치는 요청은 런타임이 거절한다."
    )


def _fallback(reason: str) -> SupervisorDecision:
    """판단할 수 없을 때의 안전한 행동.

    ``COMPLETE_INCIDENT``다. ``FAIL_INCIDENT``가 아닌 이유: 그때까지의 분석은
    실패하지 않았고, 리포트는 존재한다. 실패로 닫으면 운영자가 받는 리포트에
    붉은 배너가 붙어 내용을 의심하게 된다 — 판단 호출 하나가 실패한 것과
    분석이 실패한 것은 다르다. 그 사실은 ``closing_reason``에 남는다.
    """
    return SupervisorDecision(
        action=SupervisorAction.COMPLETE_INCIDENT,
        reason=reason,
        based_on=("runtime",),
    )


def _decide_call(
    messages: list[dict],
    max_tokens: int,
    *,
    provider: str,
    model: str,
    api_key: str,
) -> str:
    return complete(
        messages=messages,
        provider=provider,
        model=model,
        api_key=api_key,
        max_tokens=max_tokens,
        response_format=DecisionOutput,
    )
