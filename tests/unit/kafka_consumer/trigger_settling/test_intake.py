import asyncio
import time
from datetime import UTC, datetime

from cluster_doctor.incident_orchestrator_agent.service.incident_lifecycle.analyze_incident import (
    StartIncident,
)
from cluster_doctor.kafka_consumer.trigger_settling.service.intake import SlowlogIntake


class _FakeAnalyzeIncident:
    """analyze_incident.handle 자리를 대신한다.

    ``block=True``면 ``release()``를 부를 때까지 ``handle``이 멈춰 있는다 —
    "분석이 도는 동안"을 테스트에서 재현하기 위함이다.
    """

    def __init__(self, *, block: bool = False) -> None:
        self.calls: list[StartIncident] = []
        self._release = asyncio.Event()
        if not block:
            self._release.set()

    async def handle(self, command: StartIncident) -> None:
        self.calls.append(command)
        await self._release.wait()

    def release(self) -> None:
        self._release.set()


async def _wait_until(predicate, *, timeout: float = 2.0, interval: float = 0.01) -> None:
    async def _poll() -> None:
        while not predicate():
            await asyncio.sleep(interval)

    await asyncio.wait_for(_poll(), timeout=timeout)


async def test_settles_after_two_consecutive_empty_polls():
    fake = _FakeAnalyzeIncident()
    intake = SlowlogIntake(
        analyze_incident=fake, quiet_period_seconds=0.02, max_settling_wait_seconds=5
    )

    await intake.handle(datetime.now(UTC))
    await _wait_until(lambda: len(fake.calls) == 1)

    command = fake.calls[0]
    assert command.observed_start <= command.observed_end
    await intake.close()


async def test_new_arrival_mid_settle_extends_window_and_resets_streak():
    fake = _FakeAnalyzeIncident()
    intake = SlowlogIntake(
        analyze_incident=fake, quiet_period_seconds=0.03, max_settling_wait_seconds=5
    )

    t1 = datetime.now(UTC)
    await intake.handle(t1)
    # 첫 quiet period 하나만 지나가게 한다 — 아직 정착(2번 연속 0건)되지 않은 시점.
    await asyncio.sleep(0.04)

    t2 = datetime.now(UTC)
    await intake.handle(t2)

    await _wait_until(lambda: len(fake.calls) == 1, timeout=3)
    command = fake.calls[0]
    assert command.observed_end >= t2
    await intake.close()


async def test_close_cancels_pending_settle_wait_promptly():
    fake = _FakeAnalyzeIncident()
    intake = SlowlogIntake(
        analyze_incident=fake, quiet_period_seconds=5.0, max_settling_wait_seconds=60
    )

    await intake.handle(datetime.now(UTC))
    await asyncio.sleep(0.01)  # 정착 루프가 첫 sleep에 들어간 뒤

    start = time.monotonic()
    await asyncio.wait_for(intake.close(), timeout=2.0)
    elapsed = time.monotonic() - start

    assert elapsed < 1.0  # quiet_period(5초)를 기다리지 않고 바로 취소됨
    assert fake.calls == []


async def test_close_waits_for_in_flight_analysis_to_finish():
    fake = _FakeAnalyzeIncident(block=True)
    intake = SlowlogIntake(
        analyze_incident=fake, quiet_period_seconds=0.02, max_settling_wait_seconds=5
    )

    await intake.handle(datetime.now(UTC))
    await _wait_until(lambda: len(fake.calls) == 1)  # 이제 분석 중(block)

    close_task = asyncio.create_task(intake.close())
    await asyncio.sleep(0.05)
    assert not close_task.done()  # 진행 중인 분석을 중단하지 않고 기다린다

    fake.release()
    await asyncio.wait_for(close_task, timeout=2.0)
    assert close_task.done()


async def test_second_trigger_during_analysis_waits_for_first_to_finish():
    fake = _FakeAnalyzeIncident(block=True)
    intake = SlowlogIntake(
        analyze_incident=fake, quiet_period_seconds=0.02, max_settling_wait_seconds=5
    )

    await intake.handle(datetime.now(UTC))
    await _wait_until(lambda: len(fake.calls) == 1)  # 첫 Incident 분석 중(block)

    await intake.handle(datetime.now(UTC))
    await asyncio.sleep(0.1)
    assert len(fake.calls) == 1  # 두 번째 트리거는 큐에 쌓였을 뿐 아직 처리되지 않음

    fake.release()
    await _wait_until(lambda: len(fake.calls) == 2, timeout=2)
    await intake.close()
