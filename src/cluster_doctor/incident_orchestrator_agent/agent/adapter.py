"""Main DeepAgent를 ``IncidentAnalyzer`` 포트 뒤에 놓는다.

``incident_lifecycle`` 서비스는 ``deepagents``도 ``langchain``도 모른다. 아는
것은 "Incident 하나와 그 IncidentState를 맡기면 어떻게 끝났는지 돌려준다"뿐이고,
그 경계가 이 파일이다.

**그래프는 Incident마다 새로 만든다.** 도구와 미들웨어가 그 Incident의
``IncidentState``를 클로저로 쥐기 때문이다 — 프로세스 전역에 하나를 만들어
두고 incident_id를 인자로 받게 하면, 모델이 그 인자를 채우게 되고 남의
Incident 예산을 쓰는 길이 열린다. 조립 비용은 LLM 왕복 수십 번에 비하면
무시할 수 있다.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from langchain_core.messages import HumanMessage

from cluster_doctor.incident_analysis_agent.agent.session import AnalysisSeams
from cluster_doctor.incident_analysis_agent.agent.subagent import build_analysis_subagent
from cluster_doctor.incident_analysis_agent.datasource.clickhouse.client import LogRepository
from cluster_doctor.incident_analysis_agent.datasource.clickhouse.node_metric import (
    NodeMetricThresholds,
)
from cluster_doctor.incident_analysis_agent.datasource.elasticsearch.cluster_health import (
    ClusterRepository,
)
from cluster_doctor.incident_analysis_agent.datasource.elasticsearch.node_resolver import (
    NodeResolver,
)
from cluster_doctor.incident_analysis_agent.datasource.ssh.node_log import NodeLogFetcher
from cluster_doctor.incident_analysis_agent.model.basemodel.kst import format_kst
from cluster_doctor.incident_analysis_agent.service.report_generation.report_writer import (
    ReportWriter,
    build_structured_call,
)
from cluster_doctor.incident_orchestrator_agent.agent.graph import build_main_agent
from cluster_doctor.incident_orchestrator_agent.agent.middleware import (
    DelegationGuardrailMiddleware,
)
from cluster_doctor.incident_orchestrator_agent.agent.runtime.chat_model import build_chat_model
from cluster_doctor.incident_orchestrator_agent.agent.runtime.harness import restrict_harness
from cluster_doctor.incident_orchestrator_agent.agent.runtime.litellm_client import (
    require_supported_provider,
)
from cluster_doctor.incident_orchestrator_agent.agent.tools import (
    make_finish_incident_tool,
    make_list_candidate_windows_tool,
    make_propose_analysis_tool,
)
from cluster_doctor.incident_orchestrator_agent.model.basemodel.incident import (
    Incident,
    IncidentStatus,
)
from cluster_doctor.incident_orchestrator_agent.model.state.incident_state import IncidentState
from cluster_doctor.incident_orchestrator_agent.model.state.main_agent_state import (
    ADMITTED_GOAL,
    ADMITTED_WINDOW,
)
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.guardrails import (
    MAX_SUPERVISOR_CYCLES,
)
from cluster_doctor.incident_orchestrator_agent.service.incident_lifecycle.incident_analyzer import (
    IncidentAnalysisResult,
    IncidentAnalyzer,
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


@dataclass(frozen=True)
class DeepAgentsConfig:
    """Configuration owned by the composite Main Agent adapter."""

    provider: str
    model: str
    api_key: str
    heap_warn_percent: int
    queue_warn: int


def build_deepagents_incident_analyzer(
    *,
    config: DeepAgentsConfig,
    log_repository: LogRepository,
    cluster_repository: ClusterRepository,
    node_resolver: NodeResolver,
    node_log_fetcher: NodeLogFetcher,
) -> IncidentAnalyzer:
    """Assemble the complete DeepAgents engine behind the incident-analyzer port."""
    provider = require_supported_provider(config.provider)
    call_llm = build_structured_call(
        provider=provider, model=config.model, api_key=config.api_key
    )
    seams = AnalysisSeams(
        fetch_logs=log_repository.fetch_logs,
        fetch_node_logs=log_repository.fetch_node_logs,
        cluster=cluster_repository,
        node_resolver=node_resolver,
        node_log_fetcher=node_log_fetcher,
        call_llm=call_llm,
        report_writer=ReportWriter(call_llm=call_llm),
        metric_thresholds=NodeMetricThresholds(
            heap_warn_percent=config.heap_warn_percent,
            queue_warn=config.queue_warn,
        ),
    )
    return _DeepAgentIncidentAnalyzer(
        provider=provider,
        model=config.model,
        api_key=config.api_key,
        seams=seams,
    )


class _DeepAgentIncidentAnalyzer:
    """Main DeepAgent 구현. ``IncidentAnalyzer`` 포트를 만족한다."""

    def __init__(
        self,
        *,
        provider: str,
        model: str,
        api_key: str,
        seams: AnalysisSeams,
        recursion_limit: int = _RECURSION_LIMIT,
    ) -> None:
        # 모델은 한 번만 만든다. tool calling 지원 검증도 여기서 끝난다 —
        # 기동 시점에 실패해야 하고, 첫 Incident가 들어온 뒤가 아니다.
        self._model = build_chat_model(provider=provider, model=model, api_key=api_key)
        # 그래프를 만들기 **전에** 한 번 건다. Main과 Analysis가 같은 모델을
        # 쓰므로 등록 하나가 둘 다에 걸린다. 조립 순서에 기대지 않으려고
        # 여기서 부른다 — 어느 쪽이 먼저 만들어지든 이미 등록되어 있다.
        restrict_harness(self._model)
        self._seams = seams
        self._recursion_limit = recursion_limit

    def analyze(self, incident: Incident, state: IncidentState) -> IncidentAnalysisResult:
        """Incident 하나의 분석을 끝까지 진행한다. 예외를 올리지 않는다.

        ``state``는 호출부(``AnalyzeIncident``)가 만들어 넘긴 살아 있는
        객체다. 별도 저장소가 없으므로 이 함수가 직접 갱신하고, 호출부는
        반환 뒤 같은 객체를 그대로 다시 읽는다.
        """
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
                    ADMITTED_WINDOW: None,
                    ADMITTED_GOAL: "",
                },
                {"recursion_limit": self._recursion_limit},
            )
        except Exception as exc:
            return self._fallback(exc)

        return self._result_from(state)

    # ── 조립 ─────────────────────────────────────────────────────────
    def _compile(self, incident: Incident, state: IncidentState):
        """이 Incident만을 위한 그래프. 도구가 이 state를 클로저로 쥔다."""
        tools = [
            make_list_candidate_windows_tool(state=state),
            make_propose_analysis_tool(state=state),
            make_finish_incident_tool(state=state),
        ]
        middleware = [DelegationGuardrailMiddleware(state=state)]
        analysis_subagent = build_analysis_subagent(
            seams=self._seams,
            state=state,
            incident=incident,
            model=self._model,
        )
        return build_main_agent(
            model=self._model,
            tools=tools,
            middleware=middleware,
            analysis_subagent=analysis_subagent,
        )

    # ── 결과 ─────────────────────────────────────────────────────────
    def _result_from(self, state: IncidentState) -> IncidentAnalysisResult:
        """끝난 뒤의 ``IncidentState``에서 결과를 읽는다.

        그래프의 반환값이 아니라 State를 읽는 이유: 종료를 확정하는 것은
        ``finish_incident`` 도구이고, 그 도구가 쓰는 곳이 State다. 반환
        메시지를 파싱하면 모델의 문장을 믿는 셈이 된다.
        """
        if state.status is IncidentStatus.COMPLETED and not state.window_results:
            return IncidentAnalysisResult(
                status=IncidentStatus.FAILED,
                reason="구간별 리포트 없이 완료 상태가 기록됐다",
                failed=True,
            )
        if state.status.is_terminal():
            return IncidentAnalysisResult(
                status=state.status,
                reason=state.closing_reason,
                failed=state.status is IncidentStatus.FAILED,
            )

        # 검증과 명시적인 종료 없이 정상 완료를 추정하지 않는다.
        _logger.warning("Agent가 종료를 선언하지 않고 끝났다")
        return IncidentAnalysisResult(
            status=IncidentStatus.FAILED,
            reason="Agent가 종료를 선언하지 않고 끝나 분석을 마감했다",
            failed=True,
        )

    def _fallback(self, exc: Exception) -> IncidentAnalysisResult:
        """Agent 실행 오류는 확보한 보고서 유무와 무관하게 실패다."""
        _logger.exception("Agent 실행 실패")
        return IncidentAnalysisResult(
            status=IncidentStatus.FAILED,
            reason=f"Agent 실행이 {type(exc).__name__}로 끝났다",
            failed=True,
        )
