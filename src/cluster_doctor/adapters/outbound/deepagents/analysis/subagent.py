"""Analysis SubAgent — ``deepagents`` DeepAgent로 감싼 결정적 진단 파이프라인.

Main DeepAgent가 내장 ``task`` 도구로 이 SubAgent를 부르고, 이 SubAgent도
DeepAgent다. 모델이 자기 루프를 도는 것이 원래 설계와 어긋나지 않는 이유는
**루프가 도는 대상이 절차가 아니라 순서**이기 때문이다.

    task(subagent_type="analysis")
        ↓
    [DeepAgent 루프]  collect_evidence → write_report → report_insufficient
        │                    │               │
        │                    │               └─ 확장 요청만. 범위는 못 넓힌다.
        │                    └─ 초안 작성 → 미검증 리포트 저장
        └─ 기존 EvidenceCollector 한 번 그대로
        ↓
    [결정적 후처리]  _WindowOutcome 조립 → IncidentState 갱신 → LogAnalysisResponse 반환

도구는 셋뿐이고 전부 **굵다.** 기존에 돌던 결정적 Python을 LLM 도구로 쪼개
다시 쓰지 않는다는 뜻이다. 특히 아래는 도구가 아니다:

* ``workflows/minute_analysis/``의 분 단위 map→reduce — 루프 자체를 노출하면
  모델이 분을 건너뛸 수 있다. 어느 줄이 의미 있는지는 이미 그 안에서 모델이
  고르고 있고, 그것이 모델이 손댈 마지막 지점이다.
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
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableLambda
from pydantic import BaseModel, ConfigDict, Field

from cluster_doctor.adapters.outbound.deepagents.analysis.contracts import (
    AnalysisStatus,
    LogAnalysisRequest,
    LogAnalysisResponse,
)
from cluster_doctor.adapters.outbound.deepagents.analysis.graph import _drive
from cluster_doctor.adapters.outbound.deepagents.analysis.pipeline.grounding_validator import (
    GroundingValidator,
)
from cluster_doctor.adapters.outbound.deepagents.analysis.prompt_subagent import (
    build_subagent_prompt,
)
from cluster_doctor.adapters.outbound.deepagents.analysis.session import (
    AnalysisSeams,
    _AnalysisSession,
)
from cluster_doctor.adapters.outbound.deepagents.analysis.state import AnalysisAgentState
from cluster_doctor.adapters.outbound.deepagents.analysis.tools import (
    _build_tools,
    _verification_of,
)
from cluster_doctor.adapters.outbound.deepagents.runtime.harness import (
    DENY_ALL_FILESYSTEM,
    HideHarnessToolsMiddleware,
    RefuseDelegationMiddleware,
    restrict_harness,
)
from cluster_doctor.adapters.outbound.deepagents.supervisor.state import (
    ADMITTED_GOAL,
    ADMITTED_WINDOW,
    ANALYSIS_SUBAGENT,
)
from cluster_doctor.application.ports.incident_state_repository import (
    IncidentStateRepository,
)
from cluster_doctor.domain.analysis.kst import parse_kst
from cluster_doctor.domain.analysis.report import LogAnalysisStatus, VerificationStatus
from cluster_doctor.domain.analysis.report_validation import validate_report
from cluster_doctor.domain.analysis.time_range import InvalidTimeRangeError, TimeRange
from cluster_doctor.domain.analysis.validation_types import VerificationIssueType
from cluster_doctor.domain.incident.models import Incident
from cluster_doctor.domain.incident.state import IncidentState
from cluster_doctor.domain.incident.window_planner import plan_new_windows

_logger = logging.getLogger(__name__)

# 검증이 지적을 내놓았을 때 리포트를 고치고 다시 검증하는 횟수.
_MAX_VALIDATION_ROUNDS = 2
# 분석 자체가 원문과 어긋났을 때 같은 구간을 처음부터 다시 분석하는 횟수.
_MAX_REANALYSIS_ATTEMPTS = 1

_SUBAGENT_DESCRIPTION = (
    "승인된 analysis window 하나를 조사해 근거 수집·원인 분석·리포트 작성·원문 대조 "
    "검증까지 끝내고 검증된 리포트의 참조를 돌려준다. 구간은 propose_analysis가 승인받아 state에 "
    "써 둔 값을 쓰므로 description에 시각을 적어도 무시된다. description에는 "
    "'왜 이 구간을 보는가'만 적는다."
)


def build_analysis_subagent(
    *,
    seams: AnalysisSeams,
    state: IncidentState,
    repository: IncidentStateRepository,
    incident: Incident,
    model: BaseChatModel,
) -> CompiledSubAgent:
    """Incident 하나에 묶인 Analysis SubAgent를 만든다.

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
            _logger.warning("[analysis] 승인된 구간이 없어 위임을 거절한다")
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
            _logger.error("[analysis] 승인된 구간을 복원하지 못했다: %s", exc)
            return {
                "messages": [
                    AIMessage(content=f"승인된 분석 구간을 복원하지 못했다: {exc}")
                ],
                ADMITTED_WINDOW: None,
            }

        goal = _analysis_goal(agent_state)
        request = LogAnalysisRequest(
            incident_id=incident.incident_id,
            cluster=incident.cluster,
            analysis_window=window,
            state_ref=(state.report_refs[-1] if state.report_refs else None),
            analysis_goal=goal,
        )
        delegation = _AnalysisSession(window, request)

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
            state_schema=AnalysisAgentState,
            middleware=[HideHarnessToolsMiddleware(), RefuseDelegationMiddleware()],
            name=f"{ANALYSIS_SUBAGENT}_deepagent",
            permissions=DENY_ALL_FILESYSTEM,
        )

        _drive(graph, agent_state, delegation)

        result = _run_validation_loop(session=delegation, seams=seams)
        response = _assemble_response(seams, delegation, request)
        _apply_response(state, response, repository)

        result = result.model_copy(update={"analysis_summary": response.analysis_summary})
        return {
            "messages": [AIMessage(content=_message_text(result))],
            "structured_response": result,
            ADMITTED_WINDOW: None,
        }

    return CompiledSubAgent(
        name=ANALYSIS_SUBAGENT,
        description=_SUBAGENT_DESCRIPTION,
        runnable=RunnableLambda(_run, name=f"{ANALYSIS_SUBAGENT}_subagent"),
    )


def _run_validation_loop(
    *, session: _AnalysisSession, seams: AnalysisSeams
) -> LogAnalysisResponse:
    """리포트를 근거와 원문에 대조하고, 어긋나면 고쳐서 다시 대조한다.

    Deterministic 검증(``validate_report``)과 원문 대조(``GroundingValidator``)를
    함께 돌린다. 지적의 종류가 처방을 정한다.

    * 분석 불일치 → 같은 구간을 다시 분석한다(``_MAX_REANALYSIS_ATTEMPTS``회).
      그래도 남으면 MISMATCH로 내보낸다. 표현만 고쳐서는 틀린 분석이 낫지 않는다.
    * 표현 불일치·Deterministic 지적 → ``revise_report``로 고친다
      (``_MAX_VALIDATION_ROUNDS``회).
    * 검증 불가 → 고칠 수 없다. 통과도 아니므로 NOT_VERIFIED로 남긴다.

    마지막 수정본도 다시 검증한 뒤에 확정한다. 검증하지 않은 수정본에 PASSED를
    붙이지 않기 위해서다.

    LLM 도구가 아니다. 모델이 검증을 건너뛰거나 횟수를 늘릴 수 없어야 한다.
    """
    report = session.report
    if report is None:
        return LogAnalysisResponse(
            status=AnalysisStatus.FAILED,
            report_ref=None,
            verification_status=VerificationStatus.NOT_VERIFIED,
            failure_reason=_summary_without_report(session),
        )

    incident_id = session.request.incident_id
    grounding = GroundingValidator(store=seams.store, call_llm=seams.call_llm)
    blocking: list[str] = []
    unverifiable: list[str] = []
    revisions = 0

    for round_no in range(_MAX_VALIDATION_ROUNDS + 1):
        evidence = seams.store.list_evidence(incident_id)
        candidate_ids = {
            item.candidate_id
            for item in seams.store.get_observations(incident_id).candidates
        }
        deterministic = validate_report(report, evidence, candidate_ids=candidate_ids)
        found = grounding.validate(report, incident_id)

        analysis = [
            i.reason for i in found if i.issue_type is VerificationIssueType.ANALYSIS_MISMATCH
        ]
        expression = [
            i.reason for i in found if i.issue_type is VerificationIssueType.REPORT_MISMATCH
        ]
        unverifiable = [
            i.reason for i in found if i.issue_type is VerificationIssueType.UNVERIFIABLE
        ]
        blocking = [*deterministic.issues, *analysis, *expression]

        if not blocking or round_no == _MAX_VALIDATION_ROUNDS:
            break

        if analysis:
            fresh = None
            if session.reanalysis_count < _MAX_REANALYSIS_ATTEMPTS:
                session.reanalysis_count += 1
                fresh = _reanalyze_window(
                    seams=seams, session=session, focus="; ".join(analysis)
                )
            if fresh is None:
                break
            _adopt(session, fresh)
            report = session.report
            continue

        revised = seams.report_writer.revise_report(report, tuple(blocking), evidence)
        if revised is None:
            break
        revisions += 1
        report = revised

    if blocking:
        status, issues = VerificationStatus.MISMATCH, blocking
    elif unverifiable:
        status, issues = VerificationStatus.NOT_VERIFIED, unverifiable
    else:
        status, issues = VerificationStatus.PASSED, []

    final = report.model_copy(
        update={
            "verification_status": status,
            "verification_issues": tuple(issues),
            "revision_count": revisions,
        }
    )
    session.report = final
    session.report_ref = seams.store.put_report(incident_id, final)
    return LogAnalysisResponse(
        status=AnalysisStatus.COMPLETED,
        report_ref=session.report_ref,
        verification_status=status,
        failure_reason="; ".join(issues) or None,
    )


def _reanalyze_window(
    *, seams: AnalysisSeams, session: _AnalysisSession, focus: str
) -> _AnalysisSession | None:
    """같은 구간을 새 위임 상태에서 근거 수집부터 다시 돌린다.

    캐시를 이어받지 않는다. ``collect_evidence``는 위임당 한 번만 도는 도구라
    이전 세션을 재사용하면 같은 결과가 돌아온다. 순서가 고정된 두 단계이므로
    모델 루프 없이 도구를 직접 부른다.
    """
    goal = f"{session.request.analysis_goal}\n[재분석] 이전 리포트가 원문과 어긋났다: {focus}"
    request = session.request.model_copy(update={"analysis_goal": goal.strip()})
    fresh = _AnalysisSession(session.window, request)
    tools = {item.name: item for item in _build_tools(seams, fresh)}
    try:
        tools["collect_evidence"].invoke({})
        tools["write_report"].invoke({"focus": request.analysis_goal})
    except Exception:
        _logger.exception("[analysis] 구간 재분석이 실패했다")
        return None
    return fresh if fresh.report is not None else None


def _adopt(session: _AnalysisSession, fresh: _AnalysisSession) -> None:
    """재분석 결과를 위임 상태로 옮긴다. 응답 조립이 이 상태를 읽는다."""
    session.collected = fresh.collected
    session.evidence = fresh.evidence
    session.run_state = fresh.run_state
    session.draft = fresh.draft
    session.report = fresh.report
    session.report_ref = fresh.report_ref
    session.report_attempts += fresh.report_attempts


class _WindowOutcome(BaseModel):
    """``_apply_response``가 domain ``IncidentState``에 접어 넣는 내부 전용 값.

    Main Agent 모델은 이 값을 보지 않는다 — 모델이 보는 것은
    ``LogAnalysisResponse``(``contracts.py``, Main에게 반환하는 좁은 결과)다.
    ``suggested_windows``는 **제안**이다. Supervisor가 ``analyzed_windows``와
    비교해 실제로 새로 필요한 부분만 남긴다(``window_planner``). 여기서 확정하면
    SubAgent가 Scope를 쥐게 되고, 그것은 이 설계가 나눈 책임 경계를 되돌린다.
    """

    model_config = ConfigDict(frozen=True)

    status: LogAnalysisStatus

    analyzed_window: TimeRange

    suggested_windows: tuple[TimeRange, ...] = Field(default_factory=tuple)
    unresolved_gaps: tuple[TimeRange, ...] = Field(default_factory=tuple)

    report_ref: str | None = None
    verification_status: VerificationStatus = VerificationStatus.NOT_VERIFIED

    # 확보하지 못한 보조 근거를 사람이 읽을 문장으로. ``unresolved_gaps``와
    # 나누는 이유는 쓰임이 다르기 때문이다 — 저쪽은 Supervisor가 다음 범위를
    # 계산하는 값이고, 이쪽은 notifier가 배너로 그려 운영자에게 닿는 값이다.
    # 이 저장소는 빠진 사실이 반드시 운영자에게 도달하게 한다.
    gaps: tuple[str, ...] = Field(default_factory=tuple)

    # Supervisor가 읽을 한두 문단. 리포트 전문이 아니다 — 전문은 report_ref로
    # 꺼낸다.
    analysis_summary: str = ""


def _assemble_response(
    seams: AnalysisSeams,
    delegation: _AnalysisSession,
    request: LogAnalysisRequest,
) -> _WindowOutcome:
    """실제로 일어난 일에서 ``_WindowOutcome``을 만든다.

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
    except Exception as exc:
        _logger.warning("[analysis] 관측값 병합이 실패했다: %s", exc)

    status = _final_status(delegation)
    return _WindowOutcome(
        status=status,
        analyzed_window=delegation.window,
        suggested_windows=tuple(delegation.suggested),
        unresolved_gaps=_unresolved_gaps(seams, delegation),
        report_ref=delegation.report_ref,
        verification_status=_verification_of(delegation),
        gaps=tuple(run_state.gaps),
        analysis_summary=(
            seams.report_writer.summary_for_supervisor(report, run_state)
            if report is not None
            else _summary_without_report(delegation)
        ),
    )


def _apply_response(
    state: IncidentState,
    response: _WindowOutcome,
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
    if response.report_ref:
        state.report_refs.append(response.report_ref)

    for gap in response.gaps:
        if gap not in state.accumulated_gaps:
            state.accumulated_gaps.append(gap)

    state.pending_windows.extend(plan_new_windows(list(response.suggested_windows), state))
    state.unresolved_gaps.extend(plan_new_windows(list(response.unresolved_gaps), state))
    repository.save(state)


def _final_status(delegation: _AnalysisSession) -> LogAnalysisStatus:
    """어떤 상태로 끝났는가.

    수집 실패, 범위 확장 요청, 리포트 존재 여부로 결정한다.
    리포트 검증 결과는 상위 흐름에서 별도로 결정한다.
    """
    if not delegation.collected_once:
        return LogAnalysisStatus.FAILED
    if delegation.run_state.degraded and not delegation.evidence:
        return LogAnalysisStatus.FAILED
    if delegation.suggested:
        return LogAnalysisStatus.NEED_MORE_CONTEXT
    if delegation.report is None:
        return LogAnalysisStatus.FAILED
    return LogAnalysisStatus.COMPLETED


def _unresolved_gaps(
    seams: AnalysisSeams, delegation: _AnalysisSession
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


def _summary_without_report(delegation: _AnalysisSession) -> str:
    """리포트가 없을 때 Supervisor가 읽을 문장."""
    if not delegation.collected_once:
        return "진단이 근거 수집에 닿지 못한 채 끝났다. 이 구간은 분석되지 않았다."
    parts = [f"근거 {len(delegation.evidence)}건을 모았으나 리포트를 쓰지 못했다"]
    if delegation.insufficient_reason:
        parts.append(f"부족 사유: {delegation.insufficient_reason}")
    if delegation.run_state.gaps:
        parts.append(f"확보하지 못한 근거 {len(delegation.run_state.gaps)}건")
    return " / ".join(parts)


def _message_text(result: LogAnalysisResponse) -> str:
    """``messages``에 실을 한 줄.

    ``structured_response``가 있으면 부모가 보는 것은 그쪽이므로 이 문장은
    대체 경로다. 그래도 결정적으로 만든다 — 여기서 원문을 붙이면 대체 경로가
    열릴 때마다 Context 오염이 따라온다.
    """
    head = f"분석 종료 status={result.status} verification={result.verification_status}"
    if result.report_ref:
        head += f" report_ref={result.report_ref}"
    return f"{head}\n{result.analysis_summary}".strip()


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
