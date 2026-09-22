"""Main DeepAgent를 ``IncidentAgent`` 포트 뒤에 놓는다.

application 계층은 ``deepagents``도 ``langchain``도 모른다. 아는 것은
"Incident 하나를 맡기면 어떻게 끝났는지 돌려준다"뿐이고, 그 경계가 이 파일이다.

**그래프는 Incident마다 새로 만든다.** 도구와 미들웨어가 그 Incident의
``IncidentState``를 클로저로 쥐기 때문이다 — 프로세스 전역에 하나를 만들어
두고 incident_id를 인자로 받게 하면, 모델이 그 인자를 채우게 되고 남의
Incident 예산을 쓰는 길이 열린다. 조립 비용은 LLM 왕복 수십 번에 비하면
무시할 수 있다(``workflows/triage/graph.py``가 같은 판단을 이미 적어 뒀다).
"""

from __future__ import annotations

import logging

from langchain_core.messages import HumanMessage

from cluster_doctor.application.port.outbound.incident_agent import IncidentAgentResult
from cluster_doctor.storage.incident_state_store import (
    IncidentStateRepository,
)
from cluster_doctor.application.service.guardrails import MAX_SUPERVISOR_CYCLES
from cluster_doctor.domain.model.incident import Incident, IncidentStatus
from cluster_doctor.domain.model.incident_state import IncidentState
from cluster_doctor.agent.common.harness import restrict_harness
from cluster_doctor.agent.common.kst import format_kst
from cluster_doctor.infrastructure.outbound.agent.diagnosis.subagent import (
    DiagnosisSeams,
    build_diagnosis_subagent,
)
from cluster_doctor.infrastructure.outbound.agent.supervisor.chat_model import (
    build_chat_model,
)
from cluster_doctor.infrastructure.outbound.agent.supervisor.guardrail_middleware import (
    DelegationGuardrailMiddleware,
)
from cluster_doctor.infrastructure.outbound.agent.supervisor.main_agent import (
    build_main_agent,
)
from cluster_doctor.infrastructure.outbound.agent.supervisor.state import (
    ADMITTED_GOAL,
    ADMITTED_WINDOW,
    CLUSTER,
    INCIDENT_ID,
    LAST_RESPONSE,
)
from cluster_doctor.infrastructure.outbound.agent.supervisor.tools import (
    make_finish_incident_tool,
    make_list_candidate_windows_tool,
    make_propose_analysis_tool,
)

_logger = logging.getLogger(__name__)

# 한 사이클은 모델 호출 하나와 도구 호출 하나다. 거기에 조립·종료 단계 몫을
# 얹어 여유를 둔다. 이것은 예산이 아니라 **폭주 차단기**다 — 진짜 상한은
# 분 단위 예산과 위임 횟수이고 그쪽은 미들웨어가 쥔다.
_RECURSION_LIMIT = MAX_SUPERVISOR_CYCLES * 2 + 8

_KICKOFF = """Incident가 열렸다. 분석을 시작한다.

클러스터: {cluster}
트리거 시각: {trigger}

먼저 list_candidate_windows로 아직 보지 않은 구간과 남은 예산을 확인한 뒤
판단한다."""


class DeepAgentIncidentAdapter:
    """Main DeepAgent 구현. ``IncidentAgent`` 포트를 만족한다."""

    def __init__(
        self,
        *,
        provider: str,
        model: str,
        api_key: str,
        seams: DiagnosisSeams,
        state_repository: IncidentStateRepository,
        recursion_limit: int = _RECURSION_LIMIT,
    ) -> None:
        # 모델은 한 번만 만든다. tool calling 지원 검증도 여기서 끝난다 —
        # 기동 시점에 실패해야 하고, 첫 Incident가 들어온 뒤가 아니다.
        self._model = build_chat_model(provider=provider, model=model, api_key=api_key)
        # 그래프를 만들기 **전에** 한 번 건다. Main과 Diagnosis가 같은 모델을
        # 쓰므로 등록 하나가 둘 다에 걸린다. 조립 순서에 기대지 않으려고
        # 여기서 부른다 — 어느 쪽이 먼저 만들어지든 이미 등록되어 있다.
        restrict_harness(self._model)
        self._seams = seams
        self._states = state_repository
        self._recursion_limit = recursion_limit

    def run(self, incident: Incident, state: IncidentState) -> IncidentAgentResult:
        """Incident 하나의 분석을 끝까지 진행한다. 예외를 올리지 않는다."""
        graph = self._compile(incident, state)

        try:
            graph.invoke(
                {
                    "messages": [
                        HumanMessage(
                            content=_KICKOFF.format(
                                cluster=incident.cluster,
                                trigger=format_kst(incident.trigger_time),
                            )
                        )
                    ],
                    INCIDENT_ID: incident.incident_id,
                    CLUSTER: incident.cluster,
                    ADMITTED_WINDOW: None,
                    ADMITTED_GOAL: "",
                    LAST_RESPONSE: None,
                },
                {"recursion_limit": self._recursion_limit},
            )
        except Exception as exc:
            return self._fallback(incident, state, exc)

        return self._result_from(incident, state)

    # ── 조립 ─────────────────────────────────────────────────────────
    def _compile(self, incident: Incident, state: IncidentState):
        """이 Incident만을 위한 그래프. 도구가 이 state를 클로저로 쥔다."""
        tools = [
            make_list_candidate_windows_tool(state=state),
            make_propose_analysis_tool(state=state, repository=self._states),
            make_finish_incident_tool(state=state, repository=self._states),
        ]
        middleware = [
            DelegationGuardrailMiddleware(state=state, repository=self._states)
        ]
        diagnosis = build_diagnosis_subagent(
            seams=self._seams,
            state=state,
            repository=self._states,
            incident=incident,
            model=self._model,
        )
        return build_main_agent(
            model=self._model,
            tools=tools,
            middleware=middleware,
            diagnosis_subagent=diagnosis,
        )

    # ── 결과 ─────────────────────────────────────────────────────────
    def _result_from(
        self, incident: Incident, state: IncidentState
    ) -> IncidentAgentResult:
        """끝난 뒤의 ``IncidentState``에서 결과를 읽는다.

        그래프의 반환값이 아니라 State를 읽는 이유: 종료를 확정하는 것은
        ``finish_incident`` 도구이고, 그 도구가 쓰는 곳이 State다. 반환
        메시지를 파싱하면 모델의 문장을 믿는 셈이 된다.
        """
        current = self._states.get(incident.incident_id) or state

        if current.status.is_terminal():
            return IncidentAgentResult(
                status=current.status,
                reason=current.closing_reason,
                failed=current.status is IncidentStatus.FAILED,
            )

        # 모델이 finish_incident를 부르지 않고 멈췄다. 분석은 돌았을 수 있으므로
        # 실패로 보지 않는다 — 리포트에 붉은 배너를 다는 것은 분석이 깨졌을
        # 때이고, 종료 선언을 빠뜨린 것은 그것과 다르다.
        _logger.warning(
            "[incident %s] Agent가 종료를 선언하지 않고 끝났다", incident.incident_id
        )
        return IncidentAgentResult(
            status=IncidentStatus.COMPLETED,
            reason="Agent가 종료를 선언하지 않고 끝나 분석을 마감했다",
            failed=False,
        )

    def _fallback(
        self, incident: Incident, state: IncidentState, exc: Exception
    ) -> IncidentAgentResult:
        """Agent 실행이 깨졌을 때의 종료.

        **FAILED가 아니라 COMPLETED다.** 이미 확보한 근거와 관측값이 있으면
        리포트는 나가야 하고, 모델 쪽 사고를 분석 실패로 기록하면 재트리거가
        막힌다. 실제로 분석이 하나도 돌지 않았다면 그때는 실패로 본다 —
        그 구분이 ``analysis_call_count``다.
        """
        _logger.exception("[incident %s] Agent 실행 실패", incident.incident_id)
        current = self._states.get(incident.incident_id) or state
        nothing_analyzed = current.analysis_call_count == 0
        return IncidentAgentResult(
            status=IncidentStatus.FAILED if nothing_analyzed else IncidentStatus.COMPLETED,
            reason=f"Agent 실행이 {type(exc).__name__}로 끝났다",
            failed=nothing_analyzed,
        )
