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
MAX_CONSECUTIVE_RETRIGGERS = 3


@dataclass(frozen=True)
class _Arrival:
    trigger: SlowlogTrigger
    received_at: datetime


class SlowlogIntake:
    def __init__(
        self,
        *,
        diagnose_incident: DiagnoseIncident,
        cluster: str = "elasticsearch",
        micro_batch_seconds: float = 10.0,
        quiet_period_seconds: float = 15.0,
        max_settling_wait_seconds: float = MAX_TOTAL_WAIT_SECONDS,
        max_pending: int = 0,
    ) -> None:
        self._diagnose = diagnose_incident
        self._cluster = cluster
        self._micro_batch_seconds = micro_batch_seconds
        self._quiet_period_seconds = quiet_period_seconds
        self._max_settling_wait_seconds = max_settling_wait_seconds
        self._pending: queue.Queue[_Arrival] = queue.Queue(maxsize=max_pending)
        self._task: asyncio.Task | None = None
        self._running = False
        self._consecutive_retriggers = 0

    @property
    def is_idle(self) -> bool:
        return self._task is None

    @property
    def pending_count(self) -> int:
        return self._pending.qsize()

    async def handle(self, trigger: SlowlogTrigger | datetime) -> None:
        if isinstance(trigger, datetime):
            trigger = SlowlogTrigger(timestamp=trigger)
        arrival = _Arrival(trigger=trigger, received_at=datetime.now(UTC))
        try:
            self._pending.put_nowait(arrival)
        except queue.Full:
            _logger.warning("pending slowlog queue is full; dropping one trigger")
            return
        if self._running or self._task is not None:
            return
        self._task = asyncio.create_task(self._wait_and_diagnose(arrival))

    async def _wait_and_diagnose(
        self, first: _Arrival, *, delay: float | None = None
    ) -> None:
        current = asyncio.current_task()
        try:
            await asyncio.sleep(self._micro_batch_seconds if delay is None else delay)
            self._running = True
            tracker = await self._settle(first)
            incident = Incident(
                incident_id=uuid.uuid4().hex[:12],
                cluster=self._cluster,
                trigger_time=first.trigger.timestamp,
                kafka_receive_time=first.received_at,
                trigger_type=TriggerType.SLOWLOG,
            )
            outcome = await self._diagnose.handle(
                StartIncident(
                    incident,
                    tracker.first_seen,
                    tracker.last_seen,
                    tracker.total_wait_seconds,
                )
            )
            self._maybe_retrigger(outcome.analysis_failed)
        except Exception:
            _logger.exception("slowlog intake failed before diagnosis completed")
            self._consecutive_retriggers = 0
        finally:
            if self._task is current:
                self._task = None
                self._running = False

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

    def _maybe_retrigger(self, analysis_failed: bool) -> None:
        if analysis_failed or self._pending.empty():
            self._consecutive_retriggers = 0
            return
        if self._consecutive_retriggers >= MAX_CONSECUTIVE_RETRIGGERS:
            _logger.warning("consecutive slowlog retrigger cap reached")
            self._consecutive_retriggers = 0
            return
        self._consecutive_retriggers += 1
        self._running = True
        now = datetime.now(UTC)
        self._task = asyncio.create_task(
            self._wait_and_diagnose(
                _Arrival(SlowlogTrigger(timestamp=now), now),
                delay=self._micro_batch_seconds,
            )
        )
