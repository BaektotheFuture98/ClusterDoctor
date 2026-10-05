"""Accept problem-log signals, settle their arrival window, and request analysis."""

from __future__ import annotations

import asyncio
import logging
import queue
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

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
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import StartIncident

_logger = logging.getLogger(__name__)

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


class ProblemLogProcessor:
    """문제성 로그 신호를 큐에 모아 유입이 정착되면 분석을 요청한다.

    정착과 분석은 하나의 순차 루프다 — 한 Incident의 분석이 끝나야 다음
    신호의 정착 판단을 시작한다. 그 사이 새로 들어오는 신호는 큐에
    쌓일 뿐이다(겹치는 Incident는 순차로 처리한다).
    """

    def __init__(
        self,
        *,
        analyze_incident: AnalyzeIncident,
        cluster: str = "elasticsearch",
        quiet_period_seconds: float = QUIET_PERIOD_SECONDS,
        max_settling_wait_seconds: float = MAX_TOTAL_WAIT_SECONDS,
        max_pending: int = 0,
    ) -> None:
        self._analysis_service = analyze_incident
        self._cluster = cluster
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

    async def submit(self, problem_log_signal: ProblemLogSignal | datetime) -> None:
        """Enqueue a signal; analysis runs in the background processing loop."""
        if self._closed:
            raise RuntimeError("problem log processor is closed")
        if isinstance(problem_log_signal, datetime):
            problem_log_signal = ProblemLogSignal(timestamp=problem_log_signal)
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
                incident = Incident(
                    incident_id=uuid.uuid4().hex[:12],
                    cluster=self._cluster,
                    trigger_time=first.trigger.timestamp,
                    kafka_receive_time=first.received_at,
                    trigger_type=TriggerType.PROBLEM_LOG,
                )
                if self._closed:
                    return
                command = StartIncident(
                    incident,
                    tracker.first_seen,
                    tracker.last_seen,
                    tracker.total_wait_seconds,
                )
                self._in_flight_analysis = True
                try:
                    await self._analysis_service.handle(command)
                except Exception:
                    _logger.exception(
                        "incident analysis failed; settling loop will continue"
                    )
                finally:
                    self._in_flight_analysis = False
        except Exception:
            _logger.exception("problem log processor failed while settling signals")
        finally:
            if self._task is current:
                self._task = None
                if not self._closed and not self._pending.empty():
                    self._task = asyncio.create_task(self._process_pending())

    async def close(self) -> None:
        """Discard buffered work; wait for an active analysis to finish before closing."""
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
