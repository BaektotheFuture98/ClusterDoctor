"""Diagnosis SubAgent — ``deepagents`` DeepAgent로 감싼 결정적 진단 파이프라인.

Main DeepAgent가 내장 ``task`` 도구로 이 SubAgent를 부르고, 이 SubAgent도
DeepAgent다. 모델이 자기 루프를 도는 것이 원래 설계와 어긋나지 않는 이유는
**루프가 도는 대상이 절차가 아니라 순서**이기 때문이다.

    task(subagent_type="diagnosis")
        ↓
    [DeepAgent 루프]  collect_evidence → write_report → report_insufficient
        │                    │               │
        │                    │               └─ 확장 요청만. 범위는 못 넓힌다.
        │                    └─ 기존 draft → validate → revise 경로 그대로
        └─ 기존 EvidenceCollector 한 번 그대로
        ↓
    [결정적 후처리]  LogAnalysisResponse 조립 → IncidentState 갱신 → handback

도구는 셋뿐이고 전부 **굵다.** 기존에 돌던 결정적 Python을 LLM 도구로 쪼개
다시 쓰지 않는다는 뜻이다. 특히 아래는 도구가 아니다:

* ``workflows/triage/``의 분 단위 chunk→map→reduce — 루프 자체를 노출하면
  모델이 분을 건너뛸 수 있다. 어느 줄이 의미 있는지는 이미 그 안에서 모델이
  고르고 있고, 그것이 모델이 손댈 마지막 지점이다.
* ``validator.py``의 여덟 규칙 — 모델이 자기 출력을 채점하면 거의 통과한다.
* ``node_metric.py``의 임계값, 클러스터 상태 근거 구성.
* ``node_investigation``의 조건 — "마스터 로그가 노드를 지목했을 때만 SSH"는
  코드 규칙으로 남는다. 모델의 선택이 되면 비싼 원격 접속이 근거 없이 돈다.

경계에서 지키는 것은 원래와 같다.

**하나. 분석 구간은 graph state에서만 읽는다.** ``task``의 스키마는
``{description, subagent_type}``으로 고정이고 description은 모델의 산문이다.
거기서 시각을 파싱하면 Guardrail이 우회된다. 구간은 ``ADMITTED_WINDOW``에서
위임 시작 시 **한 번** 읽어 고정하고, 루프 도중 다시 읽지 않는다.

**둘. 부모에게 돌려보내는 payload에 원문이 없다.** 참조와 개수와 결정적으로
만들어진 요약뿐이다. ``domain/model/log_analysis.py``가 설명하듯 ArtifactStore
간접참조가 존재하는 이유 전체가 이것이다 — Main Agent의 Context에 raw 로그가
쌓이면 사이클마다 Context가 불어나고 비용 경계가 사라진다. 도구가 모델에게
돌려주는 값에도 근거 본문은 없다. SubAgent의 Context도 Context다.

**셋. 예산.** 미들웨어는 ``task`` 호출 하나당 예산을 한 번 선점한다. 그
선점 안에서 이 SubAgent가 자기 루프를 도므로, 루프가 무한하면 한 번 선점한
예산 안에서 비용이 배로 든다. 그래서 ``collect_evidence``는 위임당 한 번만
실제로 돌고, ``write_report``는 횟수 상한이 있으며, 그래프에도 재귀 상한이
걸려 있다.

이 runnable은 동기이고 수 분 블로킹될 수 있다.
**asyncio 이벤트 루프 스레드에서 invoke하면 안 된다** — 같은 루프가 Kafka를
소비하므로 분석 한 번이 유입 전체를 멈춘다. 스레드로 밀어내는 것은 조립
지점의 책임이다.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from deepagents import CompiledSubAgent, create_deep_agent

from cluster_doctor.infrastructure.outbound.agent.common.harness import (
    DENY_ALL_FILESYSTEM,
    HideHarnessToolsMiddleware,
    RefuseDelegationMiddleware,
    restrict_harness,
)
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableLambda
from langchain_core.tools import tool
from langgraph.errors import GraphRecursionError
from pydantic import BaseModel, Field

from cluster_doctor.application.port.outbound.artifact_store import ArtifactStore
from cluster_doctor.application.port.outbound.cluster_repository import ClusterRepository
from cluster_doctor.application.port.outbound.incident_state_repository import (
    IncidentStateRepository,
)
from cluster_doctor.application.port.outbound.node_log_fetcher import NodeLogFetcher
from cluster_doctor.application.port.outbound.node_resolver import NodeResolver
from cluster_doctor.application.service.window_planner import plan_new_windows
from cluster_doctor.contracts.evidence import Evidence
from cluster_doctor.domain.model.incident import Incident
from cluster_doctor.domain.model.incident_state import IncidentState
from cluster_doctor.domain.model.log_analysis import (
    LogAnalysisRequest,
    LogAnalysisResponse,
    LogAnalysisStatus,
    VerificationStatus,
)
from cluster_doctor.contracts.report import LogAnalysisReport
from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.clickhouse.node_log_entry import NodeLogEntry
from cluster_doctor.contracts.time_range import (
    InvalidTimeRangeError,
    TimeRange,
    split_span,
)
from cluster_doctor.infrastructure.outbound.agent.common.kst import parse_kst
from cluster_doctor.infrastructure.outbound.agent.diagnosis.collector import (
    CollectedEvidence,
    EvidenceCollector,
)
from cluster_doctor.infrastructure.outbound.agent.diagnosis.prompt_subagent import (
    build_subagent_prompt,
)
from cluster_doctor.infrastructure.outbound.agent.diagnosis.report_writer import (
    ReportWriter,
)
from cluster_doctor.infrastructure.outbound.agent.diagnosis.run_state import (
    AnalysisRunState,
)
from cluster_doctor.infrastructure.outbound.agent.diagnosis.schema import DraftReport
from cluster_doctor.infrastructure.outbound.agent.diagnosis.workflows.datasource.node_metric import (
    DEFAULT_THRESHOLDS,
    NodeMetricThresholds,
)
from cluster_doctor.infrastructure.outbound.agent.supervisor.state import (
    ADMITTED_GOAL,
    ADMITTED_WINDOW,
    CLUSTER,
    DIAGNOSIS_SUBAGENT,
    INCIDENT_ID,
    LAST_RESPONSE,
    IncidentAgentState,
)

_logger = logging.getLogger(__name__)

# ``write_report``가 실제로 돌 수 있는 횟수. 리포트 **안쪽**의 수정 횟수는
# ``MAX_REPORT_REVISIONS``가 쥐고 있고 여기서 다시 정의하지 않는다. 이 상한은
# 성격이 다르다 — 모델이 초안이 마음에 들지 않는다고 처음부터 다시 쓰는 것을
# 막는다. 한 번은 다시 쓸 수 있게 둔 이유는 근거를 보고 물음을 고쳐 잡는 것이
# 실제로 더 나은 리포트를 내기 때문이다.
_MAX_REPORT_ATTEMPTS = 2

# 그래프 재귀 상한. 도구가 실제로 하는 일은 위 상한들이 이미 묶으므로 이것은
# **아무 일도 안 하면서 도는** 모델에 대한 최후 방어다. 도구가 멱등해도 모델
# 호출 자체는 매 턴 토큰을 쓰고, 그 비용은 미들웨어가 선점한 예산 하나 안에서
# 발생한다 — 그래서 넉넉하게 잡지 않는다.
#
# 의도한 흐름은 수집 1 + 리포트 2 + 확장 요청 1 + 마무리 1 = 다섯 턴이고,
# langgraph는 한 턴을 두 superstep 남짓으로 센다. 24면 두 배의 여유가 있다.
# 상한에 닿으면 ``GraphRecursionError``가 나고, 그것을 잡아 그때까지 실제로
# 끝난 일로 결과를 조립한다 — 위임이 통째로 날아가면 이미 쓴 LLM 비용과 모은
# 근거가 함께 사라지기 때문이다.
_RECURSION_LIMIT = 24

_SUBAGENT_DESCRIPTION = (
    "승인된 analysis window 하나를 조사해 근거 수집·원인 분석·리포트·검증까지 "
    "끝내고 구조화된 결과를 돌려준다. 구간은 propose_analysis가 승인받아 state에 "
    "써 둔 값을 쓰므로 description에 시각을 적어도 무시된다. description에는 "
    "'왜 이 구간을 보는가'만 적는다."
)


# ── 조립부가 넘겨 주는 것 ───────────────────────────────────────────
@dataclass(frozen=True)
class DiagnosisSeams:
    """도구 셋이 실제로 쓰는 파이프라인 조각 전부.

    이 SubAgent가 모델에게 주는 것은 "수집"과 "리포트 작성"으로 **나뉜**
    단계다. 어느 쪽도 포트 하나로 표현되지 않는다 — 수집기는 datasource
    의존성 여덟 개를 받아야 하고, 리포트 작성은 그 의존성을 쓰지 않는다.
    그래서 구체 타입을 고르는 일은 조립 지점에 남기고, 여기서는 받은 조각을
    그대로 쓴다.

    frozen인 이유는 위임 중에 바뀔 값이 하나도 없기 때문이다. 위임마다 달라지는
    것은 ``_Delegation``이고, 그쪽은 가변이다.
    """

    store: ArtifactStore
    fetch_logs: Callable[[TimeRange], list[LogEntry]]
    fetch_node_logs: Callable[..., list[NodeLogEntry]]
    cluster: ClusterRepository
    node_resolver: NodeResolver
    node_log_fetcher: NodeLogFetcher
    call_llm: Callable[..., str]
    report_writer: ReportWriter
    metric_thresholds: NodeMetricThresholds = DEFAULT_THRESHOLDS


# ── 부모에게 돌려보내는 것 ──────────────────────────────────────────
class _WindowRef(BaseModel):
    """구간 하나를 ISO 문자열 쌍으로.

    ``TimeRange``를 그대로 싣지 않는 이유는 frozen dataclass라 JSON 직렬화
    경로를 그냥 통과하지 못하고, ``task`` 도구가 structured_response를
    JSON으로 찍어 ToolMessage에 넣기 때문이다.
    """

    start: str
    end: str

    @classmethod
    def of(cls, window: TimeRange) -> "_WindowRef":
        return cls(start=window.start.isoformat(), end=window.end.isoformat())


class DiagnosisHandback(BaseModel):
    """SubAgent → Main DeepAgent. **이 파일에서 가장 중요한 타입이다.**

    담는 것은 참조와 개수와 요약뿐이다. evidence 본문도, 로그 한 줄도, 리포트
    전문도 여기 없다 — 전문이 필요하면 ``report_ref``로 ArtifactStore에서 꺼낸다.
    필드를 하나 더 늘리고 싶을 때마다 그것이 참조인지 원문인지 먼저 묻는다.

    **모델이 채우지 않는다.** 코드가 실제로 일어난 일에서 조립한다. Main Agent가
    이 숫자로 예산과 종료를 판단하므로, 지어낼 수 있는 자리를 두면 안 된다.
    """

    status: str
    analyzed_window: _WindowRef
    report_ref: str | None = None
    verification_status: str
    # 개수만 싣는다. ref 목록 전체는 IncidentState가 들고 있고, Main Agent가
    # 다음 행동을 정하는 데 필요한 것은 "근거가 몇 개 잡혔나"뿐이다.
    evidence_ref_count: int = 0
    suggested_windows: list[_WindowRef] = Field(default_factory=list)
    unresolved_gaps: list[_WindowRef] = Field(default_factory=list)
    # 확보하지 못한 보조 근거를 사람이 읽을 문장으로. 로그 원문이 아니라
    # 파이프라인이 만든 설명이다.
    gaps: list[str] = Field(default_factory=list)
    analysis_summary: str = ""


# ── 위임 한 번 동안의 진행 상황 ─────────────────────────────────────
class _Delegation:
    """위임 하나가 실제로 어디까지 갔는가.

    도구들이 공유하는 유일한 가변 상태다. 위임마다 새로 만든다 — Incident
    하나에 위임이 여럿이고, 앞 위임의 근거가 다음 위임에 남으면
    ``collect_evidence``의 멱등성이 위임 경계를 넘어 잘못 작동한다.
    """

    def __init__(self, window: TimeRange, request: LogAnalysisRequest) -> None:
        self.window = window
        self.request = request
        self.run_state = AnalysisRunState(window)
        self.collected: CollectedEvidence | None = None
        self.evidence: list[Evidence] = []
        self.report: LogAnalysisReport | None = None
        self.report_ref: str | None = None
        self.draft: DraftReport | None = None
        self.report_attempts = 0
        self.insufficient_reason = ""
        self.suggested: list[TimeRange] = []

    @property
    def collected_once(self) -> bool:
        return self.collected is not None


def build_diagnosis_subagent(
    *,
    seams: DiagnosisSeams,
    state: IncidentState,
    repository: IncidentStateRepository,
    incident: Incident,
    model: BaseChatModel,
) -> CompiledSubAgent:
    """Incident 하나에 묶인 Diagnosis SubAgent를 만든다.

    Incident마다 새로 만든다. ``state``와 ``incident``를 클로저로 붙잡는 편이
    graph state에서 되살리는 것보다 안전하다 — ``IncidentState``는 이 프로세스
    안에서 계속 갱신되는 살아 있는 객체이고, 직렬화를 거치면 같은 Incident에
    대해 서로 다른 사본이 둘 생긴다.

    **호출 스레드 주의.** 반환되는 runnable은 동기이고 내부에서 수 분 블로킹될
    수 있다. asyncio 이벤트 루프 스레드에서 직접 ``invoke``하면 같은 루프가
    돌리는 Kafka 소비가 그동안 멈춘다. 별도 스레드로 옮기는 것은 조립 지점의
    책임이다.
    """
    def _run(agent_state: dict[str, Any]) -> dict[str, Any]:
        window_ref = agent_state.get(ADMITTED_WINDOW)
        if not window_ref:
            # Guardrail 미들웨어가 승인 없는 위임을 이미 막지만 여기서 한 번 더
            # 본다. 이 함수가 실제로 비용을 쓰는 지점이라, 방어를 한 층에만
            # 두면 그 층을 우회하는 경로가 생겼을 때 조용히 예산이 샌다.
            _logger.warning("[diagnosis] 승인된 구간이 없어 위임을 거절한다")
            return {
                "messages": [
                    AIMessage(
                        content=(
                            "승인된 분석 구간이 없어 진단을 수행하지 않았다. "
                            "propose_analysis로 구간을 먼저 승인받아야 한다."
                        )
                    )
                ]
            }

        try:
            # ``parse_kst``를 쓴다. ``propose_analysis``가 ``format_kst``로 쓴
            # 문자열에는 offset이 없어서 ``datetime.fromisoformat``으로 읽으면
            # naive가 된다. 그 구간은 aware인 ``state.analyzed_windows``와 비교
            # 불가라 ``_apply_response``의 차집합 산수가 ``TypeError``로 터지고,
            # 그 예외는 ``_drive``의 방어 밖이라 위임 하나가 통째로 날아간다.
            window = TimeRange(
                start=parse_kst(window_ref["start"]),
                end=parse_kst(window_ref["end"]),
            )
        except (KeyError, TypeError, ValueError, InvalidTimeRangeError) as exc:
            # 승인을 함께 소모한다. 남겨 두면 같은 깨진 값으로 위임이 반복된다.
            _logger.error("[diagnosis] 승인된 구간을 복원하지 못했다: %s", exc)
            return {
                "messages": [
                    AIMessage(content=f"승인된 분석 구간을 복원하지 못했다: {exc}")
                ],
                ADMITTED_WINDOW: None,
            }

        _warn_on_identity_mismatch(agent_state, incident)

        goal = _analysis_goal(agent_state)
        request = LogAnalysisRequest(
            incident_id=incident.incident_id,
            cluster=incident.cluster,
            analysis_window=window,
            # 앞선 리포트를 꺼낼 참조. 첫 호출에서는 None이다.
            state_ref=state.latest_report_ref,
            analysis_goal=goal,
        )
        delegation = _Delegation(window, request)

        # 그래프를 위임마다 새로 만든다. 도구가 ``delegation``을 클로저로
        # 붙잡으므로, 그래프를 재사용하면 동시에 도는 두 위임이 같은 진행
        # 상황을 공유하게 된다. 컴파일 비용은 분 단위 분석 앞에서 무시할 수 있다.
        # 호출부가 이미 불렀더라도 다시 부른다. 등록은 멱등이고, 이 함수를
        # 직접 쓰는 다른 호출부가 생겼을 때 기본 도구가 되살아나는 것을 막는다.
        restrict_harness(model)
        graph = create_deep_agent(
            model=model,
            tools=_build_tools(seams, delegation),
            system_prompt=build_subagent_prompt(
                cluster=incident.cluster, window=window, goal=goal
            ),
            state_schema=IncidentAgentState,
            # 진단은 위임의 끝이다. profile 등록이 키 불일치로 빠지면
            # general-purpose 위임처가 되살아나는데, Main Agent와 달리
            # 이쪽에는 그것을 거절할 Guardrail이 없었다.
            middleware=[HideHarnessToolsMiddleware(), RefuseDelegationMiddleware()],
            name=f"{DIAGNOSIS_SUBAGENT}_deepagent",
            # Main Agent와 같은 이유로 파일 접근을 거절한다. 이쪽은
            # ``create_deep_agent``이 한 번 더 불리는 자리라, 여기를 빠뜨리면
            # Main만 막히고 Diagnosis는 열린 채로 돈다 — 실제로 한동안 그랬다.
            permissions=DENY_ALL_FILESYSTEM,
        )

        _drive(graph, agent_state, delegation)

        response = _assemble_response(seams, delegation, request)
        _apply_response(state, response, repository)

        handback = _to_handback(response)
        return {
            "messages": [AIMessage(content=_message_text(handback))],
            # 모델이 쓴 것이 아니라 코드가 조립한 값이다. ``task``는 이 값을
            # JSON으로 찍어 부모의 ToolMessage에 넣는다.
            "structured_response": handback,
            # 승인을 소모한다. 승인 하나에 위임 하나 — 이것이 예산 회계가
            # 어긋나지 않는 이유다.
            ADMITTED_WINDOW: None,
            LAST_RESPONSE: handback.model_dump(mode="json"),
        }

    return CompiledSubAgent(
        name=DIAGNOSIS_SUBAGENT,
        description=_SUBAGENT_DESCRIPTION,
        runnable=RunnableLambda(_run, name=f"{DIAGNOSIS_SUBAGENT}_subagent"),
    )


def _drive(graph, agent_state: dict[str, Any], delegation: _Delegation) -> None:
    """모델 루프를 돌린다. **무슨 일이 있어도 예외를 올리지 않는다.**

    포트 계약이 "예외를 올리지 않는다"인 이유가 여기에도 그대로 있다. 루프가
    죽어도 그때까지 모은 근거와 쓴 리포트는 유효하고, 그것을 버리면 이미 쓴
    LLM 비용까지 함께 버리는 것이다. 실패는 ``gaps``에 문장으로 남아 운영자에게
    닿고, 결과 조립은 실제로 끝난 일만 보고 계속된다.
    """
    # 부모 state에서 messages는 ``task``가 이미 description으로 바꿔 놓았다.
    # 그대로 넘겨 모델이 위임 사유를 읽게 한다.
    try:
        graph.invoke(dict(agent_state), {"recursion_limit": _RECURSION_LIMIT})
    except GraphRecursionError:
        _logger.error(
            "[diagnosis] 재귀 상한 %d에 도달했다 — 그때까지 끝난 일로 결과를 만든다",
            _RECURSION_LIMIT,
        )
        delegation.run_state.mark_gap(
            "진단 루프가 상한에 도달해 중단됐다. 이 구간의 조사가 끝까지 가지 못했다."
        )
    except Exception as exc:  # noqa: BLE001 - 위 docstring 참조
        _logger.exception("[diagnosis] 진단 루프가 실패했다: %s", exc)
        delegation.run_state.mark_gap(f"진단 루프가 실패했다: {exc}")


# ── 도구 ────────────────────────────────────────────────────────────
def _build_tools(seams: DiagnosisSeams, delegation: _Delegation) -> list:
    """모델이 고를 수 있는 것 전부. 셋뿐이고 전부 굵다."""

    @tool
    def collect_evidence() -> dict:
        """승인된 구간의 모든 데이터소스를 훑어 근거를 모은다.

        클러스터 상태, slowlog, query log, 노드 지표, 마스터 로그를 보고,
        마스터 로그가 특정 노드를 지목했을 때만 그 노드에 접속해 확인한다.
        어느 소스를 어떤 순서로 볼지, 노드에 들어갈지는 이 도구 안의 규칙이
        정한다.

        **위임당 한 번만 실제로 돌아간다.** 두 번째 호출은 이미 모은 결과의
        요약을 그대로 돌려준다 — 데이터소스 조회도 LLM 선별도 다시 하지 않는다.
        같은 구간을 다시 훑어도 결과는 같고, 비용만 배로 든다.

        근거 **본문은 돌려주지 않는다.** 개수와 관측 요약뿐이다.
        """
        if delegation.collected_once:
            _logger.info("[diagnosis] 근거 수집은 이미 끝났다 — 캐시된 요약을 돌려준다")
            return _evidence_digest(delegation, cached=True)

        try:
            collector = EvidenceCollector(
                incident_id=delegation.request.incident_id,
                store=seams.store,
                fetch_logs=seams.fetch_logs,
                fetch_node_logs=seams.fetch_node_logs,
                cluster=seams.cluster,
                node_resolver=seams.node_resolver,
                node_log_fetcher=seams.node_log_fetcher,
                call_llm=seams.call_llm,
                metric_thresholds=seams.metric_thresholds,
            )
            delegation.collected = collector.collect(
                delegation.window, delegation.run_state
            )
        except Exception as exc:  # noqa: BLE001
            # 수집기는 소스별 실패를 스스로 삼키므로 여기까지 오는 것은 조립
            # 자체가 틀어진 경우다. 그래도 루프를 죽이지 않는다.
            _logger.exception("[diagnosis] 근거 수집이 실패했다: %s", exc)
            delegation.run_state.mark_gap(f"근거 수집이 실패했다: {exc}")
            delegation.collected = CollectedEvidence()

        delegation.evidence = list(delegation.collected.evidence)
        return _evidence_digest(delegation, cached=False)

    @tool
    def write_report(focus: str) -> dict:
        """모은 근거로 원인을 분석하고 리포트를 쓰고 검증한다.

        초안 작성 → 일관성 검증 → (지적이 있으면) 정해진 횟수 안에서 수정까지
        한 번에 돈다. 검증 규칙과 수정 횟수 상한은 이 도구 안에 있고 바꿀 수
        없다 — 모델이 자기 리포트를 채점하면 거의 통과하기 때문이다.

        ``focus``는 "이 근거로 무엇을 묻고 싶은가"다. 비워도 되지만, 구체적일
        수록 원인 분석이 좁게 들어간다.

        근거를 모으기 전에는 부를 수 없고, 위임당 실행 횟수에 상한이 있다.

        리포트 **전문은 돌려주지 않는다.** 검증 상태와 참조뿐이다.
        """
        if not delegation.collected_once:
            return {
                "error": "근거를 먼저 모아야 한다. collect_evidence를 부르고 다시 와라.",
            }
        if not delegation.evidence:
            return {
                "error": (
                    "근거가 하나도 없어 리포트를 쓰지 않는다. 근거 없이 쓰는 원인은 "
                    "검증에서 어차피 걸린다. report_insufficient로 올려라."
                ),
            }
        if delegation.report_attempts >= _MAX_REPORT_ATTEMPTS:
            return {
                "error": (
                    f"리포트 작성 상한 {_MAX_REPORT_ATTEMPTS}회를 이미 썼다. "
                    "마지막 리포트가 결과로 나간다."
                ),
                "verification_status": _verification_of(delegation),
                "report_ref": delegation.report_ref,
            }

        delegation.report_attempts += 1
        # focus를 analysis_goal 자리에 넣는다. 원인 분석 프롬프트가 읽는 자리가
        # 거기이고, 구간은 request에 이미 고정돼 있어 focus로 바뀌지 않는다.
        request = delegation.request.model_copy(
            update={"analysis_goal": (focus or delegation.request.analysis_goal)}
        )
        try:
            draft = seams.report_writer.draft_report(
                request, delegation.evidence, delegation.run_state
            )
            report = draft.to_domain(
                incident_id=request.incident_id,
                window=delegation.window,
                evidence_refs=tuple(item.evidence_id for item in delegation.evidence),
            )
            report = seams.report_writer.verify_and_revise(
                report, delegation.evidence, delegation.run_state.candidate_ids()
            )
            report_ref = seams.store.put_report(request.incident_id, report)
        except Exception as exc:  # noqa: BLE001
            _logger.exception("[diagnosis] 리포트 작성이 실패했다: %s", exc)
            delegation.run_state.mark_gap(f"리포트 작성이 실패했다: {exc}")
            return {"error": f"리포트 작성이 실패했다: {exc}"}

        delegation.draft = draft
        delegation.report = report
        delegation.report_ref = report_ref
        # 초안이 "구간 밖을 봐야 한다"고 말했으면 그것을 여기서 올린다.
        # 그 판단을 내린 모델은 도구가 없는 구조화 호출 안에 있어 스스로
        # report_insufficient를 부를 수 없다 — 전달하지 않으면 요청이 조용히
        # 사라지고, 프롬프트가 쓰라고 시킨 필드가 아무 데도 닿지 않는다.
        wanted = [
            f"{item.start.isoformat()}/{item.end.isoformat()}"
            for item in draft.parsed_windows()
        ]
        payload = {
            "report_ref": report_ref,
            "verification_status": str(report.verification_status),
            "verification_issue_count": len(report.verification_issues),
            "revision_count": report.revision_count,
            "finding_count": len(report.findings),
            "root_cause_count": len(report.root_causes),
            "unresolved_question_count": len(report.unresolved_questions),
            "attempts_left": _MAX_REPORT_ATTEMPTS - delegation.report_attempts,
        }
        if draft.needs_more_context and wanted:
            payload["needs_more_context"] = True
            payload["draft_suggested_windows"] = wanted
            payload["next_step"] = (
                "초안이 이 구간 밖을 봐야 한다고 판단했다. "
                "동의하면 report_insufficient에 위 구간을 그대로 넘겨라."
            )
        return payload

    @tool
    def report_insufficient(reason: str, suggested_windows: list[str]) -> dict:
        """이 구간의 근거만으로는 답할 수 없다고 상위에 올린다.

        ``suggested_windows``는 ``"<시작 ISO>/<끝 ISO>"`` 형식 문자열의 목록이다
        (예: ``"2026-09-21T13:50:00+09:00/2026-09-21T14:00:00+09:00"``).

        **제안일 뿐이다.** 실제로 그 구간을 보게 될지는 상위 Agent가 예산과 이미
        분석한 구간을 보고 정한다. 이 도구를 부른다고 네 분석 범위가 넓어지지
        않는다.
        """
        delegation.insufficient_reason = (reason or "").strip()
        accepted, rejected = _parse_windows(suggested_windows)
        delegation.suggested = accepted
        if delegation.insufficient_reason:
            delegation.run_state.mark_gap(
                f"이 구간만으로는 부족하다: {delegation.insufficient_reason}"
            )
        return {
            "accepted_windows": [
                f"{w.start.isoformat()}/{w.end.isoformat()}" for w in accepted
            ],
            "rejected": rejected,
            "note": "상위 Agent가 예산과 중복을 보고 실제로 볼 구간을 정한다.",
        }

    return [collect_evidence, write_report, report_insufficient]


def _evidence_digest(delegation: _Delegation, *, cached: bool) -> dict:
    """모델이 볼 수집 결과. **본문 없이 개수와 관측 요약만.**

    SubAgent의 Context도 Context다. 여기서 근거 본문을 돌려주면 루프가 한 바퀴
    돌 때마다 그것이 메시지로 쌓이고, 원문을 Context에서 떼어 놓으려고 만든
    ArtifactStore 간접참조가 이 경계에서 무너진다.
    """
    collected = delegation.collected or CollectedEvidence()
    return {
        "evidence_count": len(delegation.evidence),
        "investigated_nodes": list(collected.investigated_nodes),
        "failed_minute_count": len(collected.failed_minutes),
        # 관측 요약은 수치와 상태의 집계다. 원문 줄이 아니다.
        "observation_summary": delegation.run_state.summary_for_prompt(),
        "suspect_candidates": delegation.run_state.candidates_for_prompt(),
        "gaps": list(delegation.run_state.gaps),
        "cached": cached,
    }


def _parse_windows(raw: list[str]) -> tuple[list[TimeRange], list[str]]:
    """``"<start>/<end>"`` 문자열을 ``TimeRange``로. 읽지 못한 것은 버린다.

    10분을 넘는 제안은 거절하지 않고 쪼갠다 — ``schema.parsed_windows``와 같은
    이유다. 상한의 근거는 한 번의 조회가 부담하는 팬아웃 비용이지 "그 시간대를
    보면 안 된다"가 아니고, 거절하면 정당한 15분 요청이 통째로 사라진다.
    """
    accepted: list[TimeRange] = []
    rejected: list[str] = []
    for item in raw or []:
        try:
            start_text, end_text = str(item).split("/", 1)
            # ``parse_kst``를 쓴다. 모델이 offset 없이 적어 보내는 것이 예사이고,
            # ``fromisoformat``으로 읽으면 그 값이 naive가 된다. naive 구간은
            # aware인 ``state.analyzed_windows``와 비교되지 못해 ``_apply_response``
            # 의 차집합에서 ``TypeError``로 터지고, 그 예외는 이미 리포트를
            # 저장한 뒤 ``repository.save`` 앞에서 나므로 리포트가 저장소에
            # 있는데도 운영자에게 가지 않는다.
            start = parse_kst(start_text.strip())
            end = parse_kst(end_text.strip())
            pieces = split_span(start, end)
            if not pieces:
                raise ValueError("시작이 끝보다 뒤이거나 같다")
            accepted.extend(pieces)
        except Exception as exc:  # noqa: BLE001
            rejected.append(f"{item!r}: {exc}")
    return accepted, rejected


# ── 결과 조립 ───────────────────────────────────────────────────────
def _assemble_response(
    seams: DiagnosisSeams,
    delegation: _Delegation,
    request: LogAnalysisRequest,
) -> LogAnalysisResponse:
    """실제로 일어난 일에서 ``LogAnalysisResponse``를 만든다.

    모델이 무엇을 말했는지가 아니라 도구가 무엇을 끝냈는지만 본다. 모델이
    "리포트를 썼다"고 말하고 ``write_report``를 부르지 않았다면 리포트는 없고,
    결과도 그렇게 나간다.
    """
    run_state = delegation.run_state
    report = delegation.report

    # 관측값은 리포트를 못 썼어도 남긴다. 다음 위임이 읽는 값이고, 이미 조회
    # 비용을 치른 결과다.
    try:
        seams.store.merge_observations(
            request.incident_id, run_state.to_observations()
        )
    except Exception as exc:  # noqa: BLE001
        _logger.warning("[diagnosis] 관측값 병합이 실패했다: %s", exc)

    status = _final_status(delegation)
    return LogAnalysisResponse(
        status=status,
        analyzed_window=delegation.window,
        suggested_windows=tuple(delegation.suggested),
        unresolved_gaps=_unresolved_gaps(seams, delegation),
        report_ref=delegation.report_ref,
        verification_status=_verification_of(delegation),
        evidence_refs=tuple(item.evidence_id for item in delegation.evidence),
        gaps=tuple(run_state.gaps),
        analysis_summary=(
            seams.report_writer.summary_for_supervisor(report, run_state)
            if report is not None
            else _summary_without_report(delegation)
        ),
    )


def _final_status(delegation: _Delegation) -> LogAnalysisStatus:
    """어떤 상태로 끝났는가.

    우선순위가 있다. 분석이 성립하지 않은 것이 가장 무겁고, 그다음이 리포트를
    믿을 수 없는 것이며, 범위 확장 요청은 그 뒤다 — 셋이 함께 일어날 수 있고,
    Main Agent에게는 가장 무거운 것을 먼저 알려야 한다.

    맨 앞 두 줄은 모델이 루프를 도는 이 경로에만 필요하다. 근거 수집조차 부르지
    않고 끝내는 실패 방식이 여기서 처음 생겼고, 그것이 COMPLETED로 나가면
    Main Agent가 "이 구간은 봤다"로 읽는다.
    """
    if not delegation.collected_once:
        return LogAnalysisStatus.FAILED
    if delegation.run_state.degraded and not delegation.evidence:
        return LogAnalysisStatus.FAILED
    if _verification_of(delegation) is VerificationStatus.MISMATCH:
        return LogAnalysisStatus.VALIDATION_FAILED
    if delegation.suggested:
        return LogAnalysisStatus.NEED_MORE_CONTEXT
    if delegation.report is None:
        return LogAnalysisStatus.FAILED
    return LogAnalysisStatus.COMPLETED


def _verification_of(delegation: _Delegation) -> VerificationStatus:
    """리포트가 없으면 검증은 통과가 아니라 **돌지 않은** 것이다."""
    if delegation.report is None:
        return VerificationStatus.NOT_VERIFIED
    return delegation.report.verification_status


def _unresolved_gaps(
    seams: DiagnosisSeams, delegation: _Delegation
) -> tuple[TimeRange, ...]:
    """근거를 확보하지 못한 시간 범위.

    수집이 아예 돌지 않았으면 구간 전체가 미해결이다. 비워 두면 Supervisor가
    "이 구간은 봤고 빈 곳이 없다"로 읽어, 보지 않은 시간이 조용히 덮인 것으로
    남는다.
    """
    if not delegation.collected_once:
        return (delegation.window,)
    return seams.report_writer.unresolved_gaps(
        delegation.collected, delegation.run_state, delegation.window
    )


def _summary_without_report(delegation: _Delegation) -> str:
    """리포트가 없을 때 Supervisor가 읽을 문장.

    비워 두지 않는다. ``latest_analysis_summary``가 비면 Main Agent는 직전
    위임에서 아무 일도 없었던 것처럼 읽고 같은 구간을 다시 요청한다.
    """
    if not delegation.collected_once:
        return "진단이 근거 수집에 닿지 못한 채 끝났다. 이 구간은 분석되지 않았다."
    parts = [f"근거 {len(delegation.evidence)}건을 모았으나 리포트를 쓰지 못했다"]
    if delegation.insufficient_reason:
        parts.append(f"부족 사유: {delegation.insufficient_reason}")
    if delegation.run_state.gaps:
        parts.append(f"확보하지 못한 근거 {len(delegation.run_state.gaps)}건")
    return " / ".join(parts)


# ── state 갱신 ──────────────────────────────────────────────────────
def _apply_response(
    state: IncidentState,
    response: LogAnalysisResponse,
    repository: IncidentStateRepository,
) -> None:
    """분석 결과를 ``IncidentState``에 접는다.

    **예산은 여기서 세지 않는다.** ``analysis_call_count``,
    ``analyzed_minutes``, ``record_analyzed``는 Guardrail 미들웨어가 위임을
    승인하는 순간 이미 선점했다. 여기서 또 세면 한 번의 분석이 두 번으로
    회계되어 Incident의 실제 예산이 절반으로 줄고, Main Agent에게 알려 준
    잔여 예산이 거짓이 된다.
    """
    state.latest_analysis_status = response.status
    state.latest_verification_status = response.verification_status
    state.latest_analysis_summary = response.analysis_summary
    if response.report_ref:
        state.latest_report_ref = response.report_ref
    for ref in response.evidence_refs:
        if ref not in state.evidence_refs:
            state.evidence_refs.append(ref)

    # 확보하지 못한 근거는 **Incident 전체에 걸쳐 쌓는다.** notifier가 배너로
    # 그려 운영자에게 닿는 값이고, 위임마다 덮어쓰면 마지막 한 번의 gap만 남아
    # 리포트가 실제보다 완전해 보인다. Main Agent에게 돌려주는 payload에는
    # 싣지 않는다 — 다음 행동을 정하는 데 쓰이는 값이 아니다.
    for gap in response.gaps:
        if gap not in state.accumulated_gaps:
            state.accumulated_gaps.append(gap)

    # 제안과 미해결 구간은 아직 보지 않은 부분만 남긴다. 그대로 쌓으면 이미
    # 분석한 구간이 pending에 남아 종료 조건을 영원히 막는다.
    state.pending_windows.extend(plan_new_windows(list(response.suggested_windows), state))
    state.unresolved_gaps.extend(plan_new_windows(list(response.unresolved_gaps), state))
    repository.save(state)


def _to_handback(response: LogAnalysisResponse) -> DiagnosisHandback:
    return DiagnosisHandback(
        status=str(response.status),
        analyzed_window=_WindowRef.of(response.analyzed_window),
        report_ref=response.report_ref,
        verification_status=str(response.verification_status),
        evidence_ref_count=len(response.evidence_refs),
        suggested_windows=[_WindowRef.of(w) for w in response.suggested_windows],
        unresolved_gaps=[_WindowRef.of(w) for w in response.unresolved_gaps],
        gaps=list(response.gaps),
        analysis_summary=response.analysis_summary,
    )


def _message_text(handback: DiagnosisHandback) -> str:
    """``messages``에 실을 한 줄.

    ``structured_response``가 있으면 부모가 보는 것은 그쪽이므로 이 문장은
    대체 경로다. 그래도 결정적으로 만든다 — 여기서 원문을 붙이면 대체 경로가
    열릴 때마다 Context 오염이 따라온다.
    """
    head = (
        f"분석 종료 status={handback.status} "
        f"verification={handback.verification_status} "
        f"evidence={handback.evidence_ref_count}건"
    )
    if handback.report_ref:
        head += f" report_ref={handback.report_ref}"
    return f"{head}\n{handback.analysis_summary}".strip()


# ── 보조 ────────────────────────────────────────────────────────────
def _analysis_goal(agent_state: dict[str, Any]) -> str:
    """이 구간을 왜 보는가.

    ``ADMITTED_GOAL``이 우선이다. 승인 절차를 함께 통과한 값이기 때문이다.
    비어 있을 때만 ``task``의 description을 쓴다 — 산문이지만 목표 문장으로는
    쓸 수 있고, **시각을 읽어 내는 데는 절대 쓰지 않는다.**
    """
    goal = (agent_state.get(ADMITTED_GOAL) or "").strip()
    if goal:
        return goal
    for message in reversed(agent_state.get("messages") or []):
        text = getattr(message, "content", "")
        if isinstance(text, str) and text.strip():
            return text.strip()
    return ""


def _warn_on_identity_mismatch(agent_state: dict[str, Any], incident: Incident) -> None:
    """graph state의 Incident와 클로저가 붙잡은 Incident가 다르면 남긴다.

    클로저 쪽을 믿는다. ``repository``와 ``state``가 그 Incident에 묶여 있어,
    graph state를 따라가면 A의 분석 결과를 B의 상태에 저장하게 된다.
    """
    graph_id = agent_state.get(INCIDENT_ID)
    if graph_id and graph_id != incident.incident_id:
        _logger.warning(
            "[diagnosis] state의 incident_id %s가 위임 대상 %s와 다르다",
            graph_id,
            incident.incident_id,
        )
    graph_cluster = agent_state.get(CLUSTER)
    if graph_cluster and graph_cluster != incident.cluster:
        _logger.warning(
            "[diagnosis] state의 cluster %s가 위임 대상 %s와 다르다",
            graph_cluster,
            incident.cluster,
        )
