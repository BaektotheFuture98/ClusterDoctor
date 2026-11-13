"""problem-log 신호를 받아 유입을 정착시키고, health를 보며 incident 세션을 만든다."""

from __future__ import annotations

import asyncio
import logging
import queue
import uuid
from dataclasses import dataclass
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.health_point import (
    HealthPoint,
    fold_health,
    is_green,
)
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.kafka_consumer.trigger_settling.service.incident_session import (
    INCIDENT_SPAN_LIMIT,
    MAX_INCIDENTS_PER_SESSION,
    continue_session,
    decide_close,
)
from cluster_doctor.kafka_consumer.trigger_settling.service.inflow import (
    InflowTracker,
    ProblemLogSignal,
)
from cluster_doctor.incident_orchestrator_agent.model.incident import (
    Incident,
    TriggerType,
)
from cluster_doctor.incident_orchestrator_agent.service.incident_lifecycle.analyze_incident import (
    AnalyzeIncident,
)
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentClose,
    IncidentOutcome,
    SessionContext,
    StartIncident,
)

_logger = logging.getLogger(__name__)

HealthProbe = Callable[[], Awaitable[dict]]

# 정착 확인 주기. 이 주기 동안 연속 2번(SETTLED_ZERO_STREAK) 새 유입이
# 없어야 정착으로 본다.
QUIET_PERIOD_SECONDS = 30.0
# 유입이 멎기를 기다리는 전체 예산. 만성적으로 계속되는 유입 때문에
# 정착 판단이 무한정 미뤄지지 않게 막는 상한이다.
MAX_TOTAL_WAIT_SECONDS = 300


@dataclass(frozen=True)
class _Arrival:
    """정착 대기 큐에 넣는 문제성 로그 신호와 실제 수신 시각.

    로그 발생 시각과 수신 시각을 보존해 유입 추적의 기준을 구분한다.
    """

    trigger: ProblemLogSignal
    received_at: datetime


@dataclass(frozen=True)
class _Built:
    """incident 구성이 끝난 결과."""

    end: datetime
    close: IncidentClose
    history: tuple[HealthPoint, ...]
    gaps: tuple[str, ...]


class ProblemLogProcessor:
    """문제성 로그 신호를 큐에 모아 정착시키고, health를 보며 incident 세션을 만든다.

    정착·incident 구성·분석은 하나의 순차 루프다 — 한 incident의 분석이 끝나야
    큐와 health를 다시 본다. 그 사이 들어온 신호는 큐에 쌓였다가 다음 incident에
    합쳐진다. ``cluster_health``가 없으면 정착 직후 incident 하나로 끝난다.
    """

    def __init__(
        self,
        *,
        analyze_incident: AnalyzeIncident,
        cluster: str = "elasticsearch",
        # 아래 두 대기 시간은 테스트가 정착 대기를 줄이려고 덮어쓴다. 운영은 기본값.
        quiet_period_seconds: float = QUIET_PERIOD_SECONDS,
        max_settling_wait_seconds: float = MAX_TOTAL_WAIT_SECONDS,
        max_pending: int = 0,
        cluster_health: HealthProbe | None = None,
        incident_span_limit: timedelta = INCIDENT_SPAN_LIMIT,
    ) -> None:
        self._analysis_service = analyze_incident
        self._cluster = cluster
        self._cluster_health = cluster_health
        self._incident_span_limit = incident_span_limit
        self._quiet_period_seconds = quiet_period_seconds
        self._max_settling_wait_seconds = max_settling_wait_seconds
        self._pending: queue.Queue[_Arrival] = queue.Queue(maxsize=max_pending)
        self._task: asyncio.Task | None = None
        # 분석이 도는 동안 close()가 이 태스크를 취소하지 않게 하는 플래그.
        # 정착 대기(sleep) 중에는 취소해도 되지만, 분석 중에 취소하면
        # 진행 중인 Incident를 중간에 끊는 셈이 된다.
        self._in_flight_analysis = False
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None

    async def submit(self, problem_log_signal: ProblemLogSignal) -> None:
        """신호를 큐에 넣는다. 분석은 백그라운드 처리 루프에서 실행된다."""
        if self._closed:
            raise RuntimeError("problem log processor is closed")
        arrival = _Arrival(trigger=problem_log_signal, received_at=datetime.now(UTC))
        try:
            self._pending.put_nowait(arrival)
        except queue.Full:
            _logger.warning("pending problem log queue is full; dropping one signal")
            return
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._process_pending())

    async def _process_pending(self) -> None:
        current = asyncio.current_task()
        try:
            while not self._closed and not self._pending.empty():
                first = self._pending.get_nowait()
                tracker = await self._settle(first)
                if self._closed:
                    return
                await self._run_session(first, tracker)
        except Exception:
            _logger.exception("problem log processor failed while settling signals")
        finally:
            if self._task is current:
                self._task = None
                if not self._closed and not self._pending.empty():
                    self._task = asyncio.create_task(self._process_pending())

    async def _run_session(self, first: _Arrival, tracker: InflowTracker) -> None:
        if self._cluster_health is None:
            await self._handle(
                StartIncident(
                    self._new_incident(first, first.trigger.timestamp),
                    tracker.first_seen,
                    tracker.last_seen,
                    tracker.total_wait_seconds,
                )
            )
            return

        start = tracker.first_seen
        session: SessionContext | None = None
        previous: tuple[str, IncidentOutcome | None] | None = None
        sequence = 1
        while True:
            built = await self._build_incident(tracker, start)
            if self._closed:
                return
            last_allowed = sequence >= MAX_INCIDENTS_PER_SESSION
            if session is None or previous is None:
                session = SessionContext(
                    session_id=uuid.uuid4().hex[:12],
                    sequence=1,
                    last_allowed=last_allowed,
                    onset=tracker.first_seen,
                )
            else:
                previous_id, previous_outcome = previous
                session = session.following(
                    incident_id=previous_id,
                    report=previous_outcome.report if previous_outcome else None,
                    windows=_analyzed_windows(previous_outcome),
                    last_allowed=last_allowed,
                )
            incident = self._new_incident(
                first, first.trigger.timestamp if sequence == 1 else start
            )
            outcome = await self._handle(
                StartIncident(
                    incident,
                    start,
                    max(start, min(tracker.last_seen, built.end)),
                    tracker.total_wait_seconds if sequence == 1 else 0.0,
                    incident_end=built.end,
                    close=built.close,
                    health_history=built.history,
                    health_gaps=built.gaps,
                    session=session,
                )
            )
            if self._closed:
                return
            queued = not self._pending.empty()
            green = None if queued else await self._probe_green()
            if not continue_session(sequence=sequence, queued=queued, green=green):
                return
            previous = (incident.incident_id, outcome)
            start = built.end
            tracker = InflowTracker(
                first_seen=start, last_seen=start, time_basis=tracker.time_basis
            )
            sequence += 1

    async def _build_incident(self, tracker: InflowTracker, start: datetime) -> _Built:
        """큐가 비었고 green이면, 아니면 범위 상한에서 마감한다."""
        history: tuple[HealthPoint, ...] = ()
        while True:
            now = datetime.now(UTC)
            arrivals = self._drain_pending()
            if arrivals:
                tracker.observe([arrival.trigger for arrival in arrivals], now=now)
            green: bool | None = False
            if not arrivals:
                try:
                    payload = await self._cluster_health()
                    history = fold_health(history, payload, now)
                    green = is_green(payload)
                except Exception as exc:
                    _logger.warning(
                        "[session] cluster health 조회 실패: %s", type(exc).__name__
                    )
                    green = None
            decided = decide_close(
                start=start,
                now=now,
                arrived=bool(arrivals),
                green=green,
                limit=self._incident_span_limit,
            )
            if decided is not None:
                gaps = (
                    ("health 확인 불가로 incident를 마감했다",)
                    if decided.close is IncidentClose.HEALTH_UNKNOWN
                    else ()
                )
                return _Built(decided.end, decided.close, history, gaps)
            await asyncio.sleep(self._quiet_period_seconds)

    async def _probe_green(self) -> bool | None:
        try:
            return is_green(await self._cluster_health())
        except Exception as exc:
            _logger.warning("[session] cluster health 조회 실패: %s", type(exc).__name__)
            return None

    async def _handle(self, command: StartIncident) -> IncidentOutcome | None:
        self._in_flight_analysis = True
        try:
            return await self._analysis_service.handle(command)
        except Exception:
            _logger.exception("incident analysis failed; settling loop will continue")
            return None
        finally:
            self._in_flight_analysis = False

    def _new_incident(self, first: _Arrival, trigger_time: datetime) -> Incident:
        return Incident(
            incident_id=uuid.uuid4().hex[:12],
            cluster=self._cluster,
            trigger_time=trigger_time,
            kafka_receive_time=first.received_at,
            trigger_type=TriggerType.PROBLEM_LOG,
        )

    async def close(self) -> None:
        """버퍼에 쌓인 작업을 버리고, 진행 중인 분석이 끝나기를 기다린 뒤 닫는다."""
        self._closed = True
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        if self._task is not None:
            if not self._in_flight_analysis:
                self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self._task = None
        self._drain_pending()

    async def _settle(self, first: _Arrival) -> InflowTracker:
        tracker = InflowTracker.from_trigger(first.trigger.timestamp, first.received_at)
        while not tracker.settled:
            remaining = self._max_settling_wait_seconds - tracker.total_wait_seconds
            if remaining <= 0:
                break
            wait = min(self._quiet_period_seconds, remaining)
            await asyncio.sleep(wait)
            tracker.total_wait_seconds += wait
            tracker.observe(
                [arrival.trigger for arrival in self._drain_pending()],
                now=datetime.now(UTC),
            )
        return tracker

    def _drain_pending(self) -> list[_Arrival]:
        entries = []
        while True:
            try:
                entries.append(self._pending.get_nowait())
            except queue.Empty:
                return entries


def _analyzed_windows(outcome: IncidentOutcome | None) -> tuple[TimeRange, ...]:
    """이전 incident가 분석한 구간. 다음 incident가 다시 분석하지 않게 넘긴다."""
    if outcome is None:
        return ()
    return tuple(
        TimeRange(start=start, end=end)
        for start, end in outcome.diagnostics.observations.requested
    )
