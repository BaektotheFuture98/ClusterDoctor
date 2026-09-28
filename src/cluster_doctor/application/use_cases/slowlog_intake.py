"""Batch and settle slowlog triggers before starting diagnosis."""

from __future__ import annotations

import asyncio
import logging
import queue
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from cluster_doctor.application.commands import StartIncident
from cluster_doctor.application.use_cases.diagnose_incident import DiagnoseIncident
from cluster_doctor.domain.incident.guardrails import (
    MAX_SINGLE_WAIT_SECONDS,
    MAX_TOTAL_WAIT_SECONDS,
)
from cluster_doctor.domain.incident.inflow import InflowTracker
from cluster_doctor.domain.incident.models import Incident, SlowlogTrigger, TriggerType

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _Arrival:
    """정착 대기 큐에 넣는 트리거와 실제 수신 시각.

    로그 발생 시각과 수신 시각을 보존해 유입 추적의 기준을 구분한다.
    """

    trigger: SlowlogTrigger
    received_at: datetime


class SlowlogIntake:
    """유입 큐에서 slowlog를 묶고 정착 후 진단 명령 큐에 넣는다.

    진단 워커가 StartIncident를 소비하며, 큐와 활성 작업 수는 프로세스 내 상태다.
    """

    def __init__(
        self,
        *,
        diagnose_incident: DiagnoseIncident,
        cluster: str = "elasticsearch",
        micro_batch_seconds: float = 10.0,
        quiet_period_seconds: float = 15.0,
        max_settling_wait_seconds: float = MAX_TOTAL_WAIT_SECONDS,
        max_pending: int = 0,
        max_incidents: int = 100,
        worker_count: int = 1,
    ) -> None:
        if max_incidents < 1 or worker_count < 1:
            raise ValueError("max_incidents and worker_count must be positive")
        self._diagnose = diagnose_incident
        self._cluster = cluster
        self._micro_batch_seconds = micro_batch_seconds
        self._quiet_period_seconds = quiet_period_seconds
        self._max_settling_wait_seconds = max_settling_wait_seconds
        self._pending: queue.Queue[_Arrival] = queue.Queue(maxsize=max_pending)
        self._task: asyncio.Task | None = None
        self._incidents: asyncio.Queue[StartIncident | None] = asyncio.Queue(max_incidents)
        self._worker_count = worker_count
        self._workers: list[asyncio.Task[None]] = []
        self._active = 0
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None

    @property
    def is_idle(self) -> bool:
        return (
            self._task is None and self._pending.empty()
            and self._incidents.empty() and self._active == 0
        )

    @property
    def pending_count(self) -> int:
        return self._pending.qsize()

    async def handle(self, trigger: SlowlogTrigger | datetime) -> None:
        if self._closed:
            raise RuntimeError("slowlog intake is closed")
        if isinstance(trigger, datetime):
            trigger = SlowlogTrigger(timestamp=trigger)
        arrival = _Arrival(trigger=trigger, received_at=datetime.now(UTC))
        try:
            self._pending.put_nowait(arrival)
        except queue.Full:
            _logger.warning("pending slowlog queue is full; dropping one trigger")
            return
        if not self._workers:
            self._workers = [
                asyncio.create_task(self._diagnose_worker())
                for _ in range(self._worker_count)
            ]
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._publish_settled())

    async def _publish_settled(self) -> None:
        current = asyncio.current_task()
        try:
            while not self._pending.empty():
                first = self._pending.get_nowait()
                await asyncio.sleep(self._micro_batch_seconds)
                tracker = await self._settle(first)
                incident = Incident(
                    incident_id=uuid.uuid4().hex[:12],
                    cluster=self._cluster,
                    trigger_time=first.trigger.timestamp,
                    kafka_receive_time=first.received_at,
                    trigger_type=TriggerType.SLOWLOG,
                )
                await self._incidents.put(
                    StartIncident(
                        incident,
                        tracker.first_seen,
                        tracker.last_seen,
                        tracker.total_wait_seconds,
                    )
                )
        except Exception:
            _logger.exception("slowlog intake failed while settling triggers")
        finally:
            if self._task is current:
                self._task = None
                if not self._closed and not self._pending.empty():
                    self._task = asyncio.create_task(self._publish_settled())

    async def _diagnose_worker(self) -> None:
        while True:
            command = await self._incidents.get()
            if command is None:
                self._incidents.task_done()
                return
            if self._closed:
                self._incidents.task_done()
                continue
            self._active += 1
            try:
                await self._diagnose.handle(command)
            except Exception:
                _logger.exception("incident diagnosis failed; worker will continue")
            finally:
                self._active -= 1
                self._incidents.task_done()

    async def close(self) -> None:
        """Discard buffered work and wait for active diagnoses before closing."""
        self._closed = True
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self._task = None
        self._drain_pending()
        while not self._incidents.empty():
            self._incidents.get_nowait()
            self._incidents.task_done()
        for _ in self._workers:
            await self._incidents.put(None)
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()

    async def _settle(self, first: _Arrival) -> InflowTracker:
        tracker = InflowTracker.from_trigger(first.trigger.timestamp, first.received_at)
        while not tracker.settled:
            remaining = self._max_settling_wait_seconds - tracker.total_wait_seconds
            if remaining <= 0:
                break
            wait = min(self._quiet_period_seconds, MAX_SINGLE_WAIT_SECONDS, remaining)
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
