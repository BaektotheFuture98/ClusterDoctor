"""Main DeepAgent를 ``IncidentAnalyzer`` 포트 뒤에 놓는다.

``incident_lifecycle`` 서비스는 ``deepagents``도 ``langchain``도 모른다. 아는
것은 "Incident 입력 DTO를 맡기면 결과 DTO를 돌려준다"뿐이고,
그 경계가 이 파일이다.

그래프는 Incident마다 조립하고 State는 실행 입력에서 초기화한다.
도구와 미들웨어는 State를 보관하지 않고 ToolRuntime에서 읽는다.
종료 시 마지막으로 커밋된 State를 framework-independent DTO로 투영한다.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from langchain_core.messages import HumanMessage

from cluster_doctor.incident_analysis_agent.agent.dependencies import AnalysisSeams
from cluster_doctor.incident_orchestrator_agent.agent.analysis_subagent import (
    build_analysis_subagent,
)
from cluster_doctor.incident_analysis_agent.datasource.clickhouse.client import (
    LogRepository,
)
from cluster_doctor.incident_analysis_agent.datasource.clickhouse.node_metric import (
    NodeMetricThresholds,
)
from cluster_doctor.incident_analysis_agent.datasource.elasticsearch.cluster_health import (
    ClusterRepository,
)
from cluster_doctor.incident_analysis_agent.datasource.elasticsearch.node_resolver import (
    NodeResolver,
)
from cluster_doctor.incident_analysis_agent.datasource.ssh.node_log import (
    NodeLogFetcher,
)
from cluster_doctor.incident_analysis_agent.model.kst import format_kst
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisStatus,
    VerificationStatus,
)
from cluster_doctor.incident_analysis_agent.service.report_generation.report_writer import (
    ReportWriter,
    build_structured_call,
)
from cluster_doctor.incident_orchestrator_agent.agent.graph import build_main_agent
from cluster_doctor.incident_orchestrator_agent.agent.middleware import (
    DelegationGuardrailMiddleware,
)
from cluster_doctor.incident_orchestrator_agent.agent.runtime.chat_model import (
    build_chat_model,
)
from cluster_doctor.incident_analysis_agent.agent.runtime.harness import (
    restrict_harness,
)
from cluster_doctor.incident_analysis_agent.agent.runtime.litellm_client import (
    require_supported_provider,
)
from cluster_doctor.incident_orchestrator_agent.agent.tools import (
    make_finish_incident_tool,
    make_list_candidate_windows_tool,
    make_propose_analysis_tool,
)
from cluster_doctor.incident_orchestrator_agent.model.incident import (
    Incident,
    IncidentStatus,
)
from cluster_doctor.incident_orchestrator_agent.agent.state import (
    initial_main_agent_state,
)
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.guardrails import (
    MAX_SUPERVISOR_CYCLES,
)
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.window_planner import (
    initial_windows,
)
from cluster_doctor.incident_orchestrator_agent.service.incident_lifecycle.incident_analyzer import (
    IncidentAnalyzer,
)
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentAnalysisRequest,
    IncidentAnalysisResult,
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

    def analyze(self, request: IncidentAnalysisRequest) -> IncidentAnalysisResult:
        """Incident 하나의 분석을 끝까지 진행한다. 예외를 올리지 않는다.

        호출부와 mutable State를 공유하지 않는다. 예외가 나도 마지막
        커밋된 snapshot에서 확보한 근거와 리포트를 반환한다.
        """
        incident = request.incident

        final_state = {
            "messages": [
                HumanMessage(
                    content=_KICKOFF.format(
                        cluster=incident.cluster,
                        trigger=format_kst(incident.trigger_time),
                    )
                )
            ],
            **initial_main_agent_state(
                incident_id=incident.incident_id,
                observed_end=request.observed_end,
                pending_windows=tuple(
                    initial_windows(request.observed_start, request.observed_end)
                ),
                total_wait_seconds=request.settling_wait_seconds,
            ),
        }
        try:
            graph = self._compile(incident)
            for snapshot in graph.stream(
                final_state,
                {"recursion_limit": self._recursion_limit},
                stream_mode="values",
            ):
                final_state = snapshot
        except Exception as exc:
            _logger.exception("Agent 실행 실패")
            final_state = {
                **final_state,
                "status": IncidentStatus.FAILED,
                "closing_reason": f"Agent 실행이 {type(exc).__name__}로 끝났다",
            }

        return self._result_from(final_state)

    # ── 조립 ─────────────────────────────────────────────────────────
    def _compile(self, incident: Incident):
        """Incident 입력과 고정 의존성으로 그래프를 조립한다."""
        tools = [
            make_list_candidate_windows_tool(),
            make_propose_analysis_tool(),
            make_finish_incident_tool(),
        ]
        middleware = [DelegationGuardrailMiddleware()]
        analysis_subagent = build_analysis_subagent(
            seams=self._seams,
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
    def _result_from(self, state: dict) -> IncidentAnalysisResult:
        """끝난 뒤의 ``MainAgentState``에서 결과를 읽는다.

        그래프의 반환값이 아니라 State를 읽는 이유: 종료를 확정하는 것은
        ``finish_incident`` 도구이고, 그 도구가 쓰는 곳이 State다. 반환
        메시지를 파싱하면 모델의 문장을 믿는 셈이 된다.
        """
        status, reason = state["status"], state["closing_reason"]
        if status is IncidentStatus.COMPLETED and not state["window_results"]:
            status, reason = (
                IncidentStatus.FAILED,
                "구간별 리포트 없이 완료 상태가 기록됐다",
            )
        elif not status.is_terminal():
            status, reason = (
                IncidentStatus.FAILED,
                "Agent가 종료를 선언하지 않고 끝나 분석을 마감했다",
            )
        verification_gaps = tuple(
            f"리포트 검증 불일치: {issue}"
            for item in state["window_results"]
            for issue in item.report.verification_issues
        )
        return IncidentAnalysisResult(
            status=status,
            reason=reason,
            failed=(
                status is IncidentStatus.FAILED
                or state.get("latest_analysis_status") is LogAnalysisStatus.FAILED
                or state.get("latest_verification_status")
                is VerificationStatus.MISMATCH
            ),
            gaps=tuple(dict.fromkeys((*state["accumulated_gaps"], *verification_gaps))),
            report=state["window_results"][-1].report
            if state["window_results"]
            else None,
            observations=state["observations"],
            evidence=tuple(state["evidence"]),
            analysis_calls=state["analysis_call_count"],
        )
