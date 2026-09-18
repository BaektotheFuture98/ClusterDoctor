"""Incident 하나의 Lifecycle을 돌린다.

    유입 정착 대기 (예산 안에서)
        ↓
    초기 pending_windows
        ↓
    ┌── Supervisor.decide ──> Decision
    │        ↓ REQUEST_ANALYSIS
    │   Guardrail (예산·중복·창 길이)
    │        ↓
    │   LogAnalysisAgent.analyze ──> LogAnalysisResponse
    │        ↓
    │   IncidentState 갱신
    └────────┘  (COMPLETE/FAIL/CANCEL 또는 상한까지)
        ↓
    최종 리포트 전달

**루프와 상한이 여기 있다.** 모델에게 루프를 맡기면 상한이 프롬프트 문장이
되고, 프롬프트 문장은 강제가 아니다. 모델이 정하는 것은 "다음에 무엇을 볼
것인가"와 "이제 충분한가"뿐이다.

Supervisor는 raw 로그를 받지 않는다. 이 클래스가 주고받는 것은 구조화된
Request/Response와 참조뿐이고, 실체는 ``ArtifactStore``에 있다.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from cluster_doctor.application.exception import GuardrailViolation
from cluster_doctor.application.port.outbound.artifact_store import ArtifactStore
from cluster_doctor.application.port.outbound.incident_state_repository import (
    IncidentStateRepository,
)
from cluster_doctor.application.port.outbound.log_analysis_agent import LogAnalysisAgent
from cluster_doctor.application.port.outbound.notifier import Notifier
from cluster_doctor.application.port.outbound.supervisor_agent import SupervisorAgent
from cluster_doctor.application.service.guardrails import (
    INCIDENT_TIMEOUT_SECONDS,
    MAX_REJECTED_DECISIONS,
    MAX_SUPERVISOR_CYCLES,
    CancellationToken,
    Deadline,
    check_analysis_budget,
    check_not_duplicate,
    clamp_wait,
    fit_to_budget,
    window_minutes,
)
from cluster_doctor.application.service.inflow import InflowTracker
from cluster_doctor.application.service.report_assembler import to_diagnosis_report
from cluster_doctor.application.service.window_planner import (
    initial_windows,
    plan_new_windows,
)
from cluster_doctor.domain.model.incident import Incident, IncidentStatus
from cluster_doctor.domain.model.incident_state import IncidentState
from cluster_doctor.domain.model.log_analysis import (
    LogAnalysisRequest,
    LogAnalysisResponse,
    LogAnalysisStatus,
    VerificationStatus,
)
from cluster_doctor.domain.model.supervisor_decision import (
    SupervisorAction,
    SupervisorDecision,
)
from cluster_doctor.domain.model.time_range import TimeRange

_logger = logging.getLogger(__name__)

# 유입 정착을 기다리는 1회 간격.
_WAIT_STEP_SECONDS = 15.0


@dataclass
class IncidentOutcome:
    """Incident 하나가 어떻게 끝났는가.

    ``analysis_failed``가 재트리거를 막는 유일한 조건이다. 근거가 일부 빠진 것
    (``gaps``)은 분석이 성공한 것이므로 막지 않는다.
    """

    incident_id: str
    status: IncidentStatus
    analysis_failed: bool = False
    gaps: tuple[str, ...] = ()
    report_ref: str | None = None
    analysis_calls: int = 0
    reason: str = ""


class IncidentOrchestrator:
    """Supervisor 사이클과 런타임 상한을 소유한다."""

    def __init__(
        self,
        *,
        supervisor: SupervisorAgent,
        log_analysis_agent: LogAnalysisAgent,
        state_repository: IncidentStateRepository,
        artifact_store: ArtifactStore,
        notifier: Notifier,
        drain_pending,
        max_cycles: int = MAX_SUPERVISOR_CYCLES,
        incident_timeout_seconds: float = INCIDENT_TIMEOUT_SECONDS,
        wait_step_seconds: float = _WAIT_STEP_SECONDS,
    ) -> None:
        self._supervisor = supervisor
        self._agent = log_analysis_agent
        self._states = state_repository
        self._store = artifact_store
        self._notifier = notifier
        self._drain_pending = drain_pending
        self._max_cycles = max_cycles
        self._incident_timeout_seconds = incident_timeout_seconds
        self._wait_step_seconds = wait_step_seconds

    async def run(
        self, incident: Incident, *, cancellation: CancellationToken | None = None
    ) -> IncidentOutcome:
        """Incident 하나를 끝까지 돌린다. 예외를 올리지 않는다."""
        token = cancellation or CancellationToken()
        deadline = Deadline(self._incident_timeout_seconds)

        state = IncidentState(incident_id=incident.incident_id)
        self._states.create(state)

        tracker = await self._settle_inflow(incident, state, token=token, deadline=deadline)
        state.pending_windows = initial_windows(tracker.first_seen, tracker.last_seen)
        state.total_wait_seconds = tracker.total_wait_seconds
        state.status = IncidentStatus.ANALYZING
        self._states.save(state)

        gaps: list[str] = []
        analysis_failed = False
        last_response: LogAnalysisResponse | None = None

        for cycle in range(self._max_cycles):
            if token.is_cancelled:
                self._close(state, IncidentStatus.CANCELLED, token.reason or "취소됨")
                break
            if deadline.expired:
                self._close(
                    state,
                    IncidentStatus.FAILED,
                    f"실행 시간 상한 {self._incident_timeout_seconds:.0f}초를 넘겼다",
                )
                analysis_failed = True
                break

            candidates = self._candidate_windows(state, last_response)
            decision = await asyncio.to_thread(
                self._supervisor.decide,
                incident,
                state,
                last_response=last_response,
                candidate_windows=candidates,
            )

            if not decision.is_request():
                self._apply_terminal(state, decision)
                analysis_failed = analysis_failed or (
                    state.status is IncidentStatus.FAILED
                )
                break

            window = self._admit(decision, state)
            if window is None:
                state.rejected_decision_count += 1
                self._states.save(state)
                if state.rejected_decision_count >= MAX_REJECTED_DECISIONS:
                    self._close(
                        state,
                        IncidentStatus.COMPLETED,
                        "Supervisor의 요청이 연속으로 런타임 제약에 걸려 종료했다",
                    )
                    break
                continue

            _logger.info(
                "[incident %s] 사이클 %d — %s ~ %s 분석 요청",
                incident.incident_id,
                cycle + 1,
                window.start.strftime("%H:%M"),
                window.end.strftime("%H:%M"),
            )
            request = LogAnalysisRequest(
                incident_id=incident.incident_id,
                cluster=incident.cluster,
                analysis_window=window,
                state_ref=state.latest_report_ref,
                analysis_goal=decision.analysis_goal,
            )
            response = await asyncio.to_thread(self._agent.analyze, request)
            last_response = response
            gaps.extend(response.gaps)
            if response.status is LogAnalysisStatus.FAILED:
                analysis_failed = True
            self._apply_response(state, window, response)
        else:
            self._close(
                state,
                state.status
                if state.status.is_terminal()
                else IncidentStatus.COMPLETED,
                f"Supervisor 사이클 상한 {self._max_cycles}회에 도달했다",
            )

        if not state.status.is_terminal():
            self._close(state, IncidentStatus.COMPLETED, state.closing_reason)

        await self._deliver(incident, state, analysis_failed, tuple(gaps))

        outcome = IncidentOutcome(
            incident_id=incident.incident_id,
            status=state.status,
            analysis_failed=analysis_failed
            or state.latest_verification_status is VerificationStatus.MISMATCH,
            gaps=tuple(gaps),
            report_ref=state.latest_report_ref,
            analysis_calls=state.analysis_call_count,
            reason=state.closing_reason,
        )
        _logger.info(
            "[incident %s] 종료 status=%s calls=%d gaps=%d reason=%s",
            incident.incident_id,
            outcome.status,
            outcome.analysis_calls,
            len(outcome.gaps),
            outcome.reason or "-",
        )
        return outcome

    # ── 유입 정착 ────────────────────────────────────────────────────
    async def _settle_inflow(
        self,
        incident: Incident,
        state: IncidentState,
        *,
        token: CancellationToken,
        deadline: Deadline,
    ) -> InflowTracker:
        """유입이 멎을 때까지, **예산 안에서만** 기다린다.

        큐를 비우는 것이 이 루프뿐이다. 기다리지 않고 바로 분석하면 사고가
        진행 중인 구간의 절반만 보게 되고, 무한정 기다리면 분석이 시작되지
        않는다. 상한은 코드가 강제한다.
        """
        tracker = InflowTracker.start(incident.trigger_time, incident.kafka_receive_time)
        while not tracker.settled:
            if token.is_cancelled or deadline.expired:
                break
            wait = clamp_wait(self._wait_step_seconds, state)
            if wait <= 0:
                _logger.info("[incident %s] 대기 예산 소진 — 즉시 분석한다", incident.incident_id)
                break
            await asyncio.sleep(wait)
            state.total_wait_seconds += wait
            tracker.total_wait_seconds = state.total_wait_seconds
            count = tracker.observe(self._drain_pending(), now=datetime.now(timezone.utc))
            _logger.info(
                "[incident %s] 유입 확인 %d건 (zero_streak=%d, 누적 대기 %.0f초)",
                incident.incident_id,
                count,
                tracker.zero_streak,
                state.total_wait_seconds,
            )
        return tracker

    # ── 사이클 보조 ──────────────────────────────────────────────────
    def _candidate_windows(
        self, state: IncidentState, last_response: LogAnalysisResponse | None
    ) -> tuple[TimeRange, ...]:
        """아직 보지 않은 구간. **코드가 계산한다.**

        제안·미해결 구간·대기 구간을 합쳐 이미 분석한 것을 뺀다. 이 산수를
        모델에게 시키면 틀리고, 틀린 것이 중복 분석이면 가장 비싼 자원을
        헛되이 태운다.
        """
        proposed: list[TimeRange] = list(state.pending_windows)
        proposed.extend(state.unresolved_gaps)
        if last_response is not None:
            proposed.extend(last_response.suggested_windows)
            proposed.extend(last_response.unresolved_gaps)
        return tuple(plan_new_windows(proposed, state))

    def _admit(
        self, decision: SupervisorDecision, state: IncidentState
    ) -> TimeRange | None:
        """Guardrail을 통과한 실제 분석 구간. 거절되면 ``None``.

        **부분 중복은 잘라서 통과시킨다.** 13:50~14:05를 요청받았고 14:00~14:10이
        이미 분석됐다면 13:50~14:00으로 좁힌다 — 요청 전체를 거절하면 모델이
        똑같은 요청을 다시 내놓고 사이클만 태운다.
        """
        window = decision.analysis_window
        if window is None:
            return None
        try:
            check_analysis_budget(state)
        except GuardrailViolation as exc:
            _logger.warning("[guardrail] %s", exc)
            return None

        remaining = state.remaining_of(window)
        if not remaining:
            _logger.warning(
                "[guardrail] %s~%s 구간은 이미 분석했다 — 요청을 거절한다",
                window.start.strftime("%H:%M"),
                window.end.strftime("%H:%M"),
            )
            return None

        admitted = remaining[0]
        try:
            admitted = fit_to_budget(admitted, state)
        except GuardrailViolation as exc:
            _logger.warning("[guardrail] %s", exc)
            return None

        if admitted != window:
            _logger.info(
                "[guardrail] 요청 %s~%s 중 아직 보지 않은 %s~%s만 분석한다",
                window.start.strftime("%H:%M"),
                window.end.strftime("%H:%M"),
                admitted.start.strftime("%H:%M"),
                admitted.end.strftime("%H:%M"),
            )
        try:
            check_not_duplicate(admitted, state)
        except GuardrailViolation as exc:  # pragma: no cover - remaining이 이미 보장한다
            _logger.warning("[guardrail] %s", exc)
            return None
        return admitted

    def _apply_response(
        self, state: IncidentState, window: TimeRange, response: LogAnalysisResponse
    ) -> None:
        state.analysis_call_count += 1
        # **구간 길이로 회계한다.** 실제로 비어 있지 않았던 분만 세면 더
        # 정확하지만, 사전 검사는 조회 전에 해야 하므로 길이로 할 수밖에 없다.
        # 검사와 회계의 단위가 다르면 Supervisor에게 알려 준 잔여 예산이
        # 실제와 어긋나고, 그러면 모델이 예산을 계획에 쓸 수 없다.
        state.analyzed_minutes += window_minutes(window)
        state.record_analyzed(window)
        state.latest_analysis_status = response.status
        state.latest_verification_status = response.verification_status
        state.latest_analysis_summary = response.analysis_summary
        if response.report_ref:
            state.latest_report_ref = response.report_ref
        for ref in response.evidence_refs:
            if ref not in state.evidence_refs:
                state.evidence_refs.append(ref)

        # 제안과 미해결 구간은 **아직 보지 않은 부분만** 남긴다. 그대로 쌓으면
        # 이미 분석한 구간이 pending에 남아 종료 조건을 영원히 막는다.
        state.pending_windows.extend(
            plan_new_windows(list(response.suggested_windows), state)
        )
        state.unresolved_gaps.extend(
            plan_new_windows(list(response.unresolved_gaps), state)
        )
        self._states.save(state)

    def _apply_terminal(self, state: IncidentState, decision: SupervisorDecision) -> None:
        mapping = {
            SupervisorAction.COMPLETE_INCIDENT: IncidentStatus.COMPLETED,
            SupervisorAction.FAIL_INCIDENT: IncidentStatus.FAILED,
            SupervisorAction.CANCEL_INCIDENT: IncidentStatus.CANCELLED,
        }
        self._close(state, mapping[decision.action], decision.reason)

    def _close(self, state: IncidentState, status: IncidentStatus, reason: str) -> None:
        state.status = status
        state.closing_reason = reason
        self._states.save(state)

    # ── 전달 ─────────────────────────────────────────────────────────
    async def _deliver(
        self,
        incident: Incident,
        state: IncidentState,
        analysis_failed: bool,
        gaps: tuple[str, ...],
    ) -> None:
        """리포트를 운영자에게 전달한다. **항상 전달한다.**

        분석이 실패했더라도 코드가 모은 관측값이 있으면 그 시각에 무슨 일이
        있었는지는 남는다. 여기서 예외를 올리면 리포트가 사라지고 운영자는
        ``logs/app.log``를 뒤져야 한다 — 실패를 알리는 일은 배너가 맡는다.
        """
        observations = self._store.get_observations(incident.incident_id)
        report = (
            self._store.get_report(state.latest_report_ref)
            if state.latest_report_ref
            else None
        )
        evidence = self._store.list_evidence(incident.incident_id)

        all_gaps = list(gaps)
        if report is not None and report.verification_issues:
            all_gaps.extend(
                f"리포트 검증 불일치: {issue}" for issue in report.verification_issues
            )
        if state.closing_reason and state.status is not IncidentStatus.COMPLETED:
            all_gaps.append(f"Incident 종료 사유: {state.closing_reason}")

        try:
            await self._notifier.notify(
                to_diagnosis_report(report, observations, evidence),
                gaps=tuple(all_gaps),
                analysis_failed=analysis_failed,
            )
        except Exception:
            _logger.exception("[incident %s] 리포트 전달 실패", incident.incident_id)
