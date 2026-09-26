"""Kafka slowlog 수신 시 Incident를 만들어 Supervisor를 돌린다.

micro_batch_seconds 동안 slowlog를 모은 뒤 Incident 하나를 연다.
  - 첫 slowlog 수신 → 타이머 시작, pending 큐에 적재
  - 타이머 만료 → Incident 생성 → ``IncidentRunner.run``
  - 실행 중 도착한 slowlog → pending 큐에 적재 (runner가 유입 정착을
    확인하며 직접 꺼낸다)
  - 성공으로 끝났고 큐에 잔여 항목이 남아 있으면 재트리거. 단
    micro_batch_seconds만큼 쉰 뒤에 걸고, 연속 3회를 넘기지 않는다.
    실패한 실행은 재트리거하지 않는다 (다음 slowlog 도착 시 자연히 재개된다).

**Incident 경계가 여기 있다.** 재트리거는 새 Incident다 — 앞선 Incident의
State도 Evidence도 이어받지 않는다. slowlog는 몰려서 오므로 한 건마다 진단하면
같은 사고를 수십 번 분석하게 되고, 그 묶음이 Incident 하나다.
"""

import asyncio
import logging
import queue as stdlib_queue
import uuid
from collections.abc import Coroutine
from datetime import datetime, timezone

from cluster_doctor.incident.runner import IncidentRunner
from cluster_doctor.domain.incident.models import Incident, TriggerType
from cluster_doctor.ingestion.kafka.event import SlowlogTriggerEvent

_logger = logging.getLogger(__name__)

# runner가 큐를 비우지 않은 채 계속 성공하면 재트리거가 끝나지 않는다.
# 상한을 둬서 무한 루프가 되지 않게 한다.
_MAX_CONSECUTIVE_RETRIGGERS = 3


class SlowlogTriggerService:
    def __init__(
        self,
        runner: IncidentRunner,
        pending: stdlib_queue.Queue,
        cluster: str = "elasticsearch",
        micro_batch_seconds: float = 10.0,
    ) -> None:
        self._runner = runner
        self._pending = pending
        self._cluster = cluster
        self._micro_batch_seconds = micro_batch_seconds
        self._running = False
        self._trigger_task: asyncio.Task | None = None
        self._incident_task: asyncio.Task | None = None
        self._consecutive_retriggers = 0

    async def on_slowlog(self, log_entry: SlowlogTriggerEvent) -> None:
        """Kafka consumer가 slowlog를 수신할 때마다 호출한다."""
        try:
            self._pending.put_nowait(log_entry)
        except stdlib_queue.Full:
            _logger.warning(
                "pending 큐가 가득 찼다 (maxsize=%d). slowlog 1건을 버린다.",
                self._pending.maxsize,
            )
            return

        if self._running:
            return

        if self._trigger_task is None:
            self._trigger_task = asyncio.create_task(
                self._wait_and_trigger(
                    log_entry.timestamp, datetime.now(timezone.utc)
                )
            )

    async def _wait_and_trigger(
        self, log_time: datetime, kafka_receive_time: datetime
    ) -> None:
        """micro_batch_seconds 후 Incident를 연다."""
        await asyncio.sleep(self._micro_batch_seconds)
        self._trigger_task = None
        self._running = True
        self._consecutive_retriggers = 0
        self._spawn(self._run_incident(log_time, kafka_receive_time))

    def _spawn(self, coro: Coroutine) -> None:
        """태스크를 띄우고 핸들을 보관한다.

        asyncio는 실행 중인 태스크에 약한 참조만 유지한다. create_task의
        반환값을 버리면 실행 도중 GC되어 진단이 아무 흔적 없이 사라질 수 있다.
        """
        self._incident_task = asyncio.create_task(coro)

    async def _run_incident(
        self,
        log_time: datetime,
        kafka_receive_time: datetime,
        *,
        delay: float = 0,
    ) -> None:
        """delay > 0이면 먼저 그만큼 쉰다.

        재실행에 지연이 없으면 실패한 실행이 지연 0으로 연달아 돌아,
        상한에 걸릴 때까지 할당량을 그대로 태운다.
        """
        if delay:
            await asyncio.sleep(delay)
        incident = Incident(
            incident_id=uuid.uuid4().hex[:12],
            cluster=self._cluster,
            trigger_time=log_time,
            kafka_receive_time=kafka_receive_time,
            trigger_type=TriggerType.SLOWLOG,
        )
        _logger.info(
            "Incident %s 시작 (log_time=%s, kafka_receive_time=%s)",
            incident.incident_id,
            log_time.strftime("%Y-%m-%d %H:%M:%S"),
            kafka_receive_time.strftime("%Y-%m-%d %H:%M:%S"),
        )
        succeeded = False
        try:
            outcome = await self._runner.run(incident)
            # 재트리거 여부는 완전성으로만 판단한다. 근거가 일부 빠진 것(gaps)은
            # 분석이 성공한 것이므로 막지 않는다 — 큐에 남은 항목은 그 사이
            # 새로 도착한 slowlog다.
            succeeded = not outcome.analysis_failed
        except Exception:
            # exc_info를 남긴다. runner는 예외를 올리지 않기로 되어 있으므로
            # 여기 오는 것은 원인을 모르는 실패이고, 스택 없이는 진단할 수 없다.
            _logger.exception("Incident 실행 중 예상치 못한 오류")
        finally:
            self._running = False
            self._incident_task = None
            self._maybe_retrigger(succeeded)

    def _maybe_retrigger(self, succeeded: bool) -> None:
        """다음 실행을 이어서 걸지 결정한다.

        실패한 실행은 절대 이어 걸지 않는다. 실패하면 큐가 그대로 남고, 바로
        다시 걸면 같은 실패를 백오프 없이 무한 반복하며 API 할당량을 태운다.
        다음 slowlog가 도착하면 ``on_slowlog``가 새 타이머를 걸어 자연히
        재개되므로 잃는 것은 없다.
        """
        if not succeeded:
            self._consecutive_retriggers = 0
            return

        if self._pending.empty():
            self._consecutive_retriggers = 0
            return

        if self._consecutive_retriggers >= _MAX_CONSECUTIVE_RETRIGGERS:
            _logger.warning(
                "연속 재트리거 %d회에 도달해 중단한다. 다음 slowlog 도착 시 "
                "재개된다.",
                self._consecutive_retriggers,
            )
            self._consecutive_retriggers = 0
            return

        self._consecutive_retriggers += 1
        self._running = True
        now = datetime.now(timezone.utc)
        self._spawn(self._run_incident(now, now, delay=self._micro_batch_seconds))
