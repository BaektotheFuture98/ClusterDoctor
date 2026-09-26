import asyncio
from datetime import UTC, datetime, timedelta

from cluster_doctor.application.commands import StartIncident
from cluster_doctor.application.use_cases.diagnose_incident import IncidentOutcome
from cluster_doctor.application.use_cases.slowlog_intake import (
    MAX_CONSECUTIVE_RETRIGGERS,
    SlowlogIntake,
)
from cluster_doctor.domain.incident.models import IncidentStatus

TS = datetime.now(UTC) - timedelta(minutes=10)


class RecordingDiagnosis:
    def __init__(self, *, failed: bool = False, on_start=None) -> None:
        self.commands: list[StartIncident] = []
        self._failed = failed
        self._on_start = on_start

    async def handle(self, command: StartIncident) -> IncidentOutcome:
        self.commands.append(command)
        if self._on_start is not None:
            await self._on_start(len(self.commands))
        return IncidentOutcome(
            incident_id=command.incident.incident_id,
            status=IncidentStatus.COMPLETED,
            analysis_failed=self._failed,
        )


async def settle(intake: SlowlogIntake, timeout: float = 2) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.005)
        if intake.is_idle:
            return
    raise AssertionError("intake가 제한 시간 안에 idle 상태가 되지 않았다")


def intake_for(diagnosis: RecordingDiagnosis, *, batch: float = 0.005) -> SlowlogIntake:
    return SlowlogIntake(
        diagnose_incident=diagnosis,
        cluster="es-prod",
        micro_batch_seconds=batch,
        quiet_period_seconds=0.005,
    )


async def test_마이크로_배치의_slowlog는_정착한_Incident_하나로_묶인다():
    diagnosis = RecordingDiagnosis()
    intake = intake_for(diagnosis)

    await intake.handle(TS)
    await intake.handle(TS + timedelta(seconds=1))
    await settle(intake)

    assert len(diagnosis.commands) == 1
    command = diagnosis.commands[0]
    assert command.incident.cluster == "es-prod"
    assert command.observed_start == TS
    assert command.observed_end == TS + timedelta(seconds=1)
    assert command.settling_wait_seconds > 0


async def test_정착_중_도착한_slowlog가_관측_끝시각을_연장한다():
    diagnosis = RecordingDiagnosis()
    intake = intake_for(diagnosis, batch=0)

    await intake.handle(TS)
    await asyncio.sleep(0.001)
    await intake.handle(TS + timedelta(minutes=2))
    await settle(intake)

    assert diagnosis.commands[0].observed_end == TS + timedelta(minutes=2)


async def test_분석_중_도착한_slowlog는_다음_Incident로_재트리거된다():
    started = asyncio.Event()
    release = asyncio.Event()

    async def block_first_start(index: int) -> None:
        if index == 1:
            started.set()
            await release.wait()

    diagnosis = RecordingDiagnosis(on_start=block_first_start)
    intake = intake_for(diagnosis)

    await intake.handle(TS)
    await asyncio.wait_for(started.wait(), timeout=1)
    await intake.handle(TS + timedelta(minutes=1))
    release.set()
    await settle(intake)

    assert len(diagnosis.commands) == 2
    assert diagnosis.commands[1].observed_start == TS + timedelta(minutes=1)


async def test_연속_재트리거는_상한을_넘지_않는다():
    intake: SlowlogIntake

    async def enqueue_next(index: int) -> None:
        await intake.handle(TS + timedelta(minutes=index))

    diagnosis = RecordingDiagnosis(on_start=enqueue_next)
    intake = intake_for(diagnosis)

    await intake.handle(TS)
    await settle(intake)

    assert len(diagnosis.commands) == 1 + MAX_CONSECUTIVE_RETRIGGERS


async def test_분석_실패는_대기_중인_slowlog를_즉시_재실행하지_않는다():
    intake: SlowlogIntake

    async def enqueue_next(index: int) -> None:
        if index == 1:
            await intake.handle(TS + timedelta(minutes=1))

    diagnosis = RecordingDiagnosis(failed=True, on_start=enqueue_next)
    intake = intake_for(diagnosis)

    await intake.handle(TS)
    await settle(intake)

    assert len(diagnosis.commands) == 1
    assert intake.pending_count == 1
