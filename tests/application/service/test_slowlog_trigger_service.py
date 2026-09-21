"""트리거 서비스의 Incident 생성과 재실행 규칙.

Incident는 pending 큐를 orchestrator가 비운다(유입 정착 확인). 그래서 실행이
실패하면 큐가 손대지지 않은 채 남고, finally가 그대로 재실행을 걸면 같은
실패를 무한히 반복하며 API 할당량만 태운다. 429(할당량 소진)에서 실제로
성립하는 조건이다.

리포트 전달은 여기서 보지 않는다 — 그것은 Incident Lifecycle의 일이고
``test_incident_orchestrator``가 본다.
"""

import asyncio
import queue as stdlib_queue
import threading
import time
from datetime import datetime, timezone

from cluster_doctor.application.service.incident_orchestrator import IncidentOutcome
from cluster_doctor.application.service.slowlog_trigger_service import (
    _MAX_CONSECUTIVE_RETRIGGERS,
    SlowlogTriggerService,
)
from cluster_doctor.domain.model.incident import IncidentStatus, TriggerType
from cluster_doctor.domain.model.kafka.slowlog_entry import SlowlogEntry

TS = datetime(2026, 8, 28, 10, 20, tzinfo=timezone.utc)


class FakeOrchestrator:
    """Incident를 받아 결과를 돌려준다. 큐를 비우는 동작도 흉내낼 수 있다."""

    def __init__(
        self,
        *,
        analysis_failed: bool = False,
        error: Exception | None = None,
        drains: stdlib_queue.Queue | None = None,
    ) -> None:
        self.incidents: list = []
        self.starts: list[float] = []
        self._analysis_failed = analysis_failed
        self._error = error
        self._drains = drains

    async def run(self, incident, *, cancellation=None) -> IncidentOutcome:
        self.incidents.append(incident)
        self.starts.append(time.perf_counter())
        if self._error is not None:
            raise self._error
        if self._drains is not None:
            # 정상 실행은 유입 정착을 확인하며 큐를 비운다.
            while not self._drains.empty():
                self._drains.get_nowait()
        return IncidentOutcome(
            incident_id=incident.incident_id,
            status=IncidentStatus.COMPLETED,
            analysis_failed=self._analysis_failed,
        )

    @property
    def calls(self) -> int:
        return len(self.incidents)


def service_for(orchestrator, pending, micro_batch_seconds: float = 0.01):
    return SlowlogTriggerService(
        orchestrator=orchestrator,
        pending=pending,
        cluster="es-prod",
        micro_batch_seconds=micro_batch_seconds,
    )


async def settle(service, timeout: float = 5.0):
    """재트리거 연쇄가 모두 끝날 때까지 기다린다."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        await asyncio.sleep(0.01)
        if service._incident_task is None and not service._running:
            return
    raise AssertionError("Incident 태스크가 제한 시간 안에 끝나지 않았다")


class TestIncidentCreation:
    async def test_트리거가_Incident_하나를_연다(self):
        orchestrator = FakeOrchestrator()
        service = service_for(orchestrator, stdlib_queue.Queue())

        await service._run_incident(TS, TS)
        await settle(service)

        assert orchestrator.calls == 1
        incident = orchestrator.incidents[0]
        assert incident.cluster == "es-prod"
        assert incident.trigger_time == TS
        assert incident.trigger_type is TriggerType.SLOWLOG

    async def test_Incident마다_다른_id를_받는다(self):
        """새 Incident는 앞선 Incident의 State도 Evidence도 이어받지 않는다.
        같은 id를 쓰면 저장소가 그 둘을 구별하지 못한다."""
        pending = stdlib_queue.Queue()
        pending.put(SlowlogEntry(timestamp=TS))
        orchestrator = FakeOrchestrator()
        service = service_for(orchestrator, pending)

        await service._run_incident(TS, TS)
        await settle(service)

        ids = {incident.incident_id for incident in orchestrator.incidents}
        assert len(ids) == orchestrator.calls

    async def test_마이크로_배치가_지난_뒤에_연다(self):
        """slowlog는 몰려서 오므로 한 건마다 진단하면 같은 사고를 수십 번
        분석하게 된다."""
        pending = stdlib_queue.Queue()
        # 정상 실행은 유입 정착을 확인하며 큐를 비운다. 비우지 않으면 재트리거가
        # 이어져 "타이머가 한 번만 걸렸는가"를 볼 수 없다.
        orchestrator = FakeOrchestrator(drains=pending)
        service = service_for(orchestrator, pending, micro_batch_seconds=0.05)

        await service.on_slowlog(SlowlogEntry(timestamp=TS))
        assert orchestrator.calls == 0

        await asyncio.sleep(0.12)
        await settle(service)
        assert orchestrator.calls == 1

    async def test_실행_중_도착한_slowlog는_새_타이머를_걸지_않는다(self):
        orchestrator = FakeOrchestrator()
        service = service_for(orchestrator, stdlib_queue.Queue())
        service._running = True

        await service.on_slowlog(SlowlogEntry(timestamp=TS))

        assert service._trigger_task is None


class TestRetrigger:
    async def test_실패한_실행은_다시_걸지_않는다(self):
        pending = stdlib_queue.Queue()
        pending.put(SlowlogEntry(timestamp=TS))
        orchestrator = FakeOrchestrator(analysis_failed=True)
        service = service_for(orchestrator, pending)

        await service._run_incident(TS, TS)
        await settle(service)

        assert orchestrator.calls == 1
        assert not pending.empty()

    async def test_예상치_못한_예외도_다시_걸지_않는다(self):
        """orchestrator는 예외를 올리지 않기로 되어 있다. 여기 오는 것은
        원인을 모르는 실패이고, 백오프 없이 반복하면 할당량만 태운다."""
        pending = stdlib_queue.Queue()
        pending.put(SlowlogEntry(timestamp=TS))
        orchestrator = FakeOrchestrator(error=RuntimeError("ES 접속 불가"))
        service = service_for(orchestrator, pending)

        await service._run_incident(TS, TS)
        await settle(service)

        assert orchestrator.calls == 1

    async def test_연속_재트리거에_상한이_있다(self):
        """큐를 끝내 비우지 않으면 성공 경로에서도 무한히 돈다."""
        pending = stdlib_queue.Queue()
        pending.put(SlowlogEntry(timestamp=TS))
        orchestrator = FakeOrchestrator()
        service = service_for(orchestrator, pending)

        await service._run_incident(TS, TS)
        await settle(service)

        assert orchestrator.calls == 1 + _MAX_CONSECUTIVE_RETRIGGERS

    async def test_재실행_전에_배치_창만큼_쉰다(self):
        """지연이 없으면 실패한 실행이 지연 0으로 연달아 돌아, 상한에 걸릴
        때까지 할당량을 그대로 태운다."""
        pending = stdlib_queue.Queue()
        pending.put(SlowlogEntry(timestamp=TS))
        delay = 0.05
        orchestrator = FakeOrchestrator()
        service = service_for(orchestrator, pending, micro_batch_seconds=delay)

        await service._run_incident(TS, TS)
        await settle(service)

        gaps = [b - a for a, b in zip(orchestrator.starts, orchestrator.starts[1:])]
        assert gaps
        # 스케줄러 오차를 감안해 느슨하게 본다. 지연이 없으면 0에 가깝다.
        assert all(gap >= delay * 0.9 for gap in gaps), gaps

    async def test_큐를_비웠으면_다시_걸지_않는다(self):
        pending = stdlib_queue.Queue()
        pending.put(SlowlogEntry(timestamp=TS))
        orchestrator = FakeOrchestrator(drains=pending)
        service = service_for(orchestrator, pending)

        await service._run_incident(TS, TS)
        await settle(service)

        assert orchestrator.calls == 1
        assert pending.empty()

    async def test_큐가_비어_있으면_다시_걸지_않는다(self):
        orchestrator = FakeOrchestrator()
        service = service_for(orchestrator, stdlib_queue.Queue())

        await service._run_incident(TS, TS)
        await settle(service)

        assert orchestrator.calls == 1


class TestTaskHandle:
    async def test_실행_중인_태스크_핸들을_보관한다(self):
        """asyncio는 실행 중인 태스크에 약한 참조만 유지한다. 핸들을 버리면
        실행 도중 GC되어 진단이 조용히 사라질 수 있다."""
        started = threading.Event()
        release = threading.Event()

        class BlockingOrchestrator(FakeOrchestrator):
            async def run(self, incident, *, cancellation=None):
                started.set()
                await asyncio.to_thread(release.wait, 5)
                return await super().run(incident, cancellation=cancellation)

        service = service_for(BlockingOrchestrator(), stdlib_queue.Queue())
        service._running = True
        service._spawn(service._run_incident(TS, TS))

        for _ in range(500):
            if started.is_set():
                break
            await asyncio.sleep(0.01)
        assert started.is_set(), "Incident가 시작되지 않았다"

        handle = service._incident_task
        assert handle is not None

        release.set()
        await handle
