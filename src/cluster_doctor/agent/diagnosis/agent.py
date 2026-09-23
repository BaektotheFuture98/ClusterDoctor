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

* ``workflows/minute_analysis/``의 분 단위 map→reduce — 루프 자체를 노출하면
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
만들어진 요약뿐이다. ``agent/contracts.py``가 설명하듯 ArtifactStore
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
from typing import Any

from deepagents import CompiledSubAgent, create_deep_agent

from cluster_doctor.agent.common.harness import (
    DENY_ALL_FILESYSTEM,
    HideHarnessToolsMiddleware,
    RefuseDelegationMiddleware,
    restrict_harness,
)
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableLambda
from pydantic import BaseModel, Field

from cluster_doctor.storage.incident_state_store import IncidentStateRepository
from cluster_doctor.incident.window_planner import plan_new_windows
from cluster_doctor.incident.models import Incident
from cluster_doctor.incident.state import IncidentState
from cluster_doctor.agent.contracts import LogAnalysisRequest, LogAnalysisResponse
from cluster_doctor.contracts.report import LogAnalysisStatus, VerificationStatus
from cluster_doctor.contracts.time_range import InvalidTimeRangeError, TimeRange
from cluster_doctor.agent.common.kst import parse_kst
from cluster_doctor.agent.diagnosis.prompt_subagent import build_subagent_prompt
from cluster_doctor.agent.diagnosis.state import DiagnosisSeams, _Delegation
from cluster_doctor.agent.diagnosis.tools import _build_tools, _verification_of
from cluster_doctor.agent.diagnosis.graph import _drive
from cluster_doctor.agent.supervisor.state import (
    ADMITTED_GOAL,
    ADMITTED_WINDOW,
    CLUSTER,
    DIAGNOSIS_SUBAGENT,
    INCIDENT_ID,
    LAST_RESPONSE,
    IncidentAgentState,
)

_logger = logging.getLogger(__name__)

_SUBAGENT_DESCRIPTION = (
    "승인된 analysis window 하나를 조사해 근거 수집·원인 분석·리포트·검증까지 "
    "끝내고 구조화된 결과를 돌려준다. 구간은 propose_analysis가 승인받아 state에 "
    "써 둔 값을 쓰므로 description에 시각을 적어도 무시된다. description에는 "
    "'왜 이 구간을 보는가'만 적는다."
)


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
    evidence_ref_count: int = 0
    suggested_windows: list[_WindowRef] = Field(default_factory=list)
    unresolved_gaps: list[_WindowRef] = Field(default_factory=list)
    gaps: list[str] = Field(default_factory=list)
    analysis_summary: str = ""


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
            state_ref=(state.report_refs[-1] if state.report_refs else None),
            analysis_goal=goal,
        )
        delegation = _Delegation(window, request)

        # 그래프를 위임마다 새로 만든다. 도구가 ``delegation``을 클로저로
        # 붙잡으므로, 그래프를 재사용하면 동시에 도는 두 위임이 같은 진행
        # 상황을 공유하게 된다. 컴파일 비용은 분 단위 분석 앞에서 무시할 수 있다.
        restrict_harness(model)
        graph = create_deep_agent(
            model=model,
            tools=_build_tools(seams, delegation),
            system_prompt=build_subagent_prompt(
                cluster=incident.cluster, window=window, goal=goal
            ),
            state_schema=IncidentAgentState,
            middleware=[HideHarnessToolsMiddleware(), RefuseDelegationMiddleware()],
            name=f"{DIAGNOSIS_SUBAGENT}_deepagent",
            permissions=DENY_ALL_FILESYSTEM,
        )

        _drive(graph, agent_state, delegation)

        response = _assemble_response(seams, delegation, request)
        _apply_response(state, response, repository)

        handback = _to_handback(response)
        return {
            "messages": [AIMessage(content=_message_text(handback))],
            "structured_response": handback,
            ADMITTED_WINDOW: None,
            LAST_RESPONSE: handback.model_dump(mode="json"),
        }

    return CompiledSubAgent(
        name=DIAGNOSIS_SUBAGENT,
        description=_SUBAGENT_DESCRIPTION,
        runnable=RunnableLambda(_run, name=f"{DIAGNOSIS_SUBAGENT}_subagent"),
    )


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
        state.report_refs.append(response.report_ref)
    for ref in response.evidence_refs:
        if ref not in state.evidence_refs:
            state.evidence_refs.append(ref)

    for gap in response.gaps:
        if gap not in state.accumulated_gaps:
            state.accumulated_gaps.append(gap)

    state.pending_windows.extend(plan_new_windows(list(response.suggested_windows), state))
    state.unresolved_gaps.extend(plan_new_windows(list(response.unresolved_gaps), state))
    repository.save(state)


def _final_status(delegation: _Delegation) -> LogAnalysisStatus:
    """어떤 상태로 끝났는가.

    우선순위가 있다. 분석이 성립하지 않은 것이 가장 무겁고, 그다음이 리포트를
    믿을 수 없는 것이며, 범위 확장 요청은 그 뒤다 — 셋이 함께 일어날 수 있고,
    Main Agent에게는 가장 무거운 것을 먼저 알려야 한다.
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
    """리포트가 없을 때 Supervisor가 읽을 문장."""
    if not delegation.collected_once:
        return "진단이 근거 수집에 닿지 못한 채 끝났다. 이 구간은 분석되지 않았다."
    parts = [f"근거 {len(delegation.evidence)}건을 모았으나 리포트를 쓰지 못했다"]
    if delegation.insufficient_reason:
        parts.append(f"부족 사유: {delegation.insufficient_reason}")
    if delegation.run_state.gaps:
        parts.append(f"확보하지 못한 근거 {len(delegation.run_state.gaps)}건")
    return " / ".join(parts)


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
    """graph state의 Incident와 클로저가 붙잡은 Incident가 다르면 남긴다."""
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
