"""Incident 하나의 수명주기를 돌린다.

    유입 정착 대기 (예산 안에서)
        ↓
    초기 pending_windows
        ↓
    IncidentAgent.run ──> Main DeepAgent가 분석을 진행한다
        ↓                  (구간 선택·위임·종료 판단이 그 안에 있다)
    종료 상태 확정
        ↓
    리포트 전달 (항상)

**여기에 루프는 없다.** 앞선 구조에서는 이 클래스가 Supervisor를 부르고,
Guardrail을 걸고, 진단을 부르는 루프를 직접 돌았다. 지금 그 루프는 Agent
안에 있다.

그렇다고 상한이 프롬프트 문장이 된 것은 아니다. 예산·중복·위임 횟수는
Agent 구현 쪽 미들웨어가 코드로 강제하고, 이 클래스는 그 바깥의 두 가지를
쥔다 — **벽시계 상한과 취소**. 둘 다 모델이 협조하지 않아도 성립해야 하는
것이라 Agent 안에 둘 수 없다.

Agent는 raw 로그를 받지 않는다. 오가는 것은 구조화된 참조와 요약뿐이고,
실체는 ``ArtifactStore``에 있다.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from cluster_doctor.storage.artifact_store import ArtifactStore
from cluster_doctor.agent.incident_agent_port import IncidentAgent
from cluster_doctor.storage.incident_state_store import IncidentStateRepository
from cluster_doctor.reporting.notifier import Notifier
from cluster_doctor.incident.guardrails import (
    INCIDENT_TIMEOUT_SECONDS,
    CancellationToken,
    Deadline,
    clamp_wait,
)
from cluster_doctor.incident.inflow import InflowTracker
from cluster_doctor.reporting.report_assembler import to_diagnosis_report
from cluster_doctor.incident.window_planner import initial_windows
from cluster_doctor.incident.models import Incident, IncidentStatus
from cluster_doctor.incident.state import IncidentState
from cluster_doctor.agent.contracts import (
    LogAnalysisStatus,
    VerificationStatus,
)

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


class IncidentRunner:
    """Incident의 수명주기 경계와 벽시계 상한을 소유한다."""

    def __init__(
        self,
        *,
        incident_agent: IncidentAgent,
        state_repository: IncidentStateRepository,
        artifact_store: ArtifactStore,
        notifier: Notifier,
        drain_pending,
        incident_timeout_seconds: float = INCIDENT_TIMEOUT_SECONDS,
        wait_step_seconds: float = _WAIT_STEP_SECONDS,
    ) -> None:
        self._agent = incident_agent
        self._states = state_repository
        self._store = artifact_store
        self._notifier = notifier
        self._drain_pending = drain_pending
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

        analysis_failed = False
        # 런너가 직접 내린 종료. Agent 쪽 판단보다 세다 — 시간이 다 됐거나
        # 취소된 것은 모델이 협조하든 말든 성립해야 하는 사실이기 때문이다.
        forced: tuple[IncidentStatus, str] | None = None

        if token.is_cancelled:
            forced = (IncidentStatus.CANCELLED, token.reason or "취소됨")
        elif deadline.expired:
            forced = (IncidentStatus.FAILED, self._timeout_reason())
            analysis_failed = True
        else:
            analysis_failed, forced = await self._run_agent(incident, state, deadline)

        # Agent가 상태를 제자리에서 갱신하고 저장소에도 반영한다. 예산 회계가
        # 그쪽에서 일어나므로 종료 판단 전에 다시 읽는다.
        state = self._states.get(incident.incident_id) or state

        if forced is not None:
            # **재조회 결과로 덮지 않는다.** 타임아웃으로 기다리기를 끊어도
            # Agent 스레드는 계속 돌고, 그 스레드가 뒤늦게 저장한 비종료 상태를
            # 그대로 믿으면 FAILED로 닫은 Incident가 COMPLETED로 뒤집힌다 —
            # analysis_failed는 True인 채로. 배너와 상태가 어긋난다.
            self._close(state, *forced)
        elif not state.status.is_terminal():
            self._close(state, IncidentStatus.COMPLETED, state.closing_reason)

        gaps = tuple(state.accumulated_gaps)
        await self._deliver(incident, state, analysis_failed, gaps)

        outcome = IncidentOutcome(
            incident_id=incident.incident_id,
            status=state.status,
            analysis_failed=analysis_failed
            or state.latest_analysis_status is LogAnalysisStatus.FAILED
            or state.latest_verification_status is VerificationStatus.MISMATCH,
            gaps=gaps,
            report_ref=state.final_report_ref,
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

    # ── Agent 실행 ───────────────────────────────────────────────────
    async def _run_agent(
        self, incident: Incident, state: IncidentState, deadline: Deadline
    ) -> tuple[bool, tuple[IncidentStatus, str] | None]:
        """Main DeepAgent에게 분석을 맡긴다.

        ``(analysis_failed, 강제 종료)``를 돌려준다. 강제 종료가 있으면 호출부는
        저장소를 다시 읽더라도 그 값으로 닫는다.

        **별도 스레드로 민다.** Agent는 동기이고 LLM 왕복이 수 분 걸릴 수 있는데,
        같은 이벤트 루프가 Kafka를 소비한다 — 여기서 막으면 그동안 들어온
        이벤트가 쌓이기만 한다.

        시간이 다 되면 기다리기를 그만둔다. 스레드 자체는 멈추지 않는다 —
        파이썬은 남의 스레드를 죽이지 못한다. 그래도 기다리기를 끊는 이유는
        리포트가 전달되어야 하기 때문이다. 남은 스레드가 뒤늦게 저장소에 쓸
        수 있으나, 그때 Incident는 이미 종료된 뒤라 다음 판단에 쓰이지 않는다.
        """
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(self._agent.run, incident, state),
                timeout=deadline.remaining,
            )
        except asyncio.TimeoutError:
            _logger.warning(
                "[incident %s] 실행 시간 상한 초과 — 확보한 것만 전달한다",
                incident.incident_id,
            )
            return True, (IncidentStatus.FAILED, self._timeout_reason())
        except Exception:
            # 포트 계약은 예외를 올리지 않는 것이지만, 계약이 깨져도 리포트는
            # 나가야 한다. 코드가 모아 둔 관측값은 모델이 전부 실패해도 남는다.
            _logger.exception("[incident %s] Agent 실행이 예외로 끝났다", incident.incident_id)
            return True, (IncidentStatus.FAILED, "분석 Agent 실행이 예외로 끝났다")

        current = self._states.get(incident.incident_id) or state
        if not current.status.is_terminal():
            self._close(current, result.status, result.reason)
        if result.gaps:
            for gap in result.gaps:
                if gap not in current.accumulated_gaps:
                    current.accumulated_gaps.append(gap)
            self._states.save(current)
        return result.failed, None

    def _timeout_reason(self) -> str:
        return f"실행 시간 상한 {self._incident_timeout_seconds:.0f}초를 넘겼다"

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
        tracker = InflowTracker.from_trigger(incident.trigger_time, incident.kafka_receive_time)
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

    # ── 종료 ─────────────────────────────────────────────────────────
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
            self._store.get_report(state.final_report_ref)
            if state.final_report_ref
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
