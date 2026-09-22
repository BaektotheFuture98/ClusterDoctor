"""Kafka 이벤트 하나가 실제로 Incident 실행까지 도달하는가.

요구사항 1번. 각 조각은 자기 파일에서 따로 검증되지만, **사슬은 조각의 합이
아니다** — 트리거 서비스가 Runner를 부르고, Runner가 Agent를 부르고, 그
사이에 pending 큐가 공유되어 있어야 한 건이라도 분석된다. 조립이 어긋나면
어느 단위 테스트도 깨지지 않은 채 운영에서 아무것도 분석되지 않는다.

여기서 가짜는 ``IncidentAgent`` 하나다. 그 아래(모델·ClickHouse·ES·SSH)는
이 사슬의 관심사가 아니다.
"""

import asyncio
import queue as stdlib_queue
from datetime import datetime, timezone

from cluster_doctor.application.port.outbound.incident_agent import IncidentAgentResult
from cluster_doctor.application.service.incident_runner import IncidentRunner
from cluster_doctor.application.service.slowlog_trigger_service import (
    SlowlogTriggerService,
)
from cluster_doctor.domain.model.incident import IncidentStatus, TriggerType
from cluster_doctor.domain.model.log_analysis import LogAnalysisStatus
from cluster_doctor.agent.integrations.clickhouse.models import SlowlogEntry
from cluster_doctor.infrastructure.outbound.memory.in_memory_artifact_store import (
    InMemoryArtifactStore,
)
from cluster_doctor.infrastructure.outbound.memory.in_memory_incident_state_repository import (
    InMemoryIncidentStateRepository,
)

TS = datetime(2026, 9, 18, 14, 3, tzinfo=timezone.utc)


class RecordingAgent:
    def __init__(self, repository, *, behaviour=None, pending=None) -> None:
        self._repository = repository
        self._behaviour = behaviour
        # 분석 도중 도착하는 slowlog를 흉내 내려면 Agent가 큐를 만질 수 있어야
        # 한다. 재트리거 조건이 서는 유일한 경로다.
        self.pending = pending
        self.incidents: list = []
        self.drained: list[int] = []

    def run(self, incident, state):
        self.incidents.append(incident)
        # 유입 정착이 큐를 비운 뒤에 불려야 한다. 남아 있으면 사고가 진행
        # 중인 구간의 절반만 보게 된다.
        self.drained.append(len(state.pending_windows))
        if self._behaviour is not None:
            self._behaviour(len(self.incidents), state, self.pending)
        self._repository.save(state)
        return IncidentAgentResult(status=IncidentStatus.COMPLETED)


class SilentNotifier:
    def __init__(self) -> None:
        self.calls = 0

    async def notify(self, report, *, gaps=(), analysis_failed=False):
        self.calls += 1


def build(behaviour=None):
    pending: stdlib_queue.Queue = stdlib_queue.Queue()

    def drain_pending():
        items = []
        while True:
            try:
                items.append(pending.get_nowait())
            except stdlib_queue.Empty:
                return items

    repository = InMemoryIncidentStateRepository()
    agent = RecordingAgent(repository, behaviour=behaviour, pending=pending)
    notifier = SilentNotifier()
    runner = IncidentRunner(
        incident_agent=agent,
        state_repository=repository,
        artifact_store=InMemoryArtifactStore(),
        notifier=notifier,
        drain_pending=drain_pending,
        # 0으로 두면 유입 정착 루프가 한 바퀴도 돌지 않아 큐가 비워지지 않는다.
        # 큐를 비우는 것이 이 루프뿐이라, 그 경로를 끄면 사슬의 한 칸이 빠진다.
        wait_step_seconds=0.001,
    )
    service = SlowlogTriggerService(
        runner=runner,
        pending=pending,
        cluster="es-prod",
        micro_batch_seconds=0.01,
    )
    return service, agent, notifier, pending


async def settle(service, timeout: float = 5.0):
    """마이크로 배치 타이머와 Incident 태스크가 모두 끝날 때까지 기다린다.

    ``_incident_task``만 보면 타이머가 아직 돌기 전에 통과한다 — 그러면
    "아무것도 실행되지 않았다"가 성공으로 읽힌다.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        await asyncio.sleep(0.01)
        if (
            service._trigger_task is None
            and service._incident_task is None
            and not service._running
        ):
            return
    raise AssertionError("Incident 태스크가 제한 시간 안에 끝나지 않았다")


async def test_slowlog_한_건이_Incident_실행과_리포트_전달까지_간다():
    service, agent, notifier, _pending = build()

    await service.on_slowlog(SlowlogEntry(timestamp=TS))
    await settle(service)

    assert len(agent.incidents) == 1
    incident = agent.incidents[0]
    assert incident.cluster == "es-prod"
    assert incident.trigger_type is TriggerType.SLOWLOG
    assert incident.trigger_time == TS
    # 리포트는 항상 전달된다 — 사슬의 마지막 칸이다.
    assert notifier.calls == 1


async def test_마이크로_배치_안의_여러_건이_Incident_하나로_묶인다():
    """slowlog는 몰려서 온다. 한 건마다 진단하면 같은 사고를 수십 번 분석한다."""
    service, agent, _notifier, _pending = build()

    for _ in range(5):
        await service.on_slowlog(SlowlogEntry(timestamp=TS))
    await settle(service)

    assert len(agent.incidents) == 1


async def test_Agent에_닿기_전에_분석_후보_구간이_준비된다():
    service, agent, _notifier, _pending = build()

    await service.on_slowlog(SlowlogEntry(timestamp=TS))
    await settle(service)

    assert agent.drained[0] >= 1, "후보 구간 없이 Agent가 불렸다"


class TestRetriggerGate:
    """재트리거는 **분석이 실제로 성립했을 때만** 걸린다.

    실행 중에도 slowlog는 계속 도착한다. 정상 종료였고 큐에 남은 것이 있으면
    이어서 한 번 더 도는 것이 맞다 — 그 항목들은 앞선 Incident가 보지 못한
    시간대다. 그러나 분석이 실패했다면 이야기가 다르다. 실패의 원인은 대개
    데이터 경로나 할당량이고, 큐가 그대로 남은 채 곧바로 다시 걸면 같은 실패를
    백오프 없이 반복하며 남은 할당량을 태운다.

    여기서 보는 것은 그 판정이 **모델의 종료 선언이 아니라 분석 결과**를
    따르는가다. 모든 위임이 FAILED로 돌아왔는데 모델이 ``finish_incident``를
    COMPLETED로 부르는 것은 관측된 실패 모드다.
    """

    async def test_모든_위임이_실패했으면_모델이_완료로_닫아도_재트리거하지_않는다(self):
        def failed_but_closed_ok(run_index, state, pending):
            # 실행 중에 새 slowlog가 도착한다 — 재트리거 조건이 선다.
            if run_index == 1:
                pending.put(SlowlogEntry(timestamp=TS))
            state.latest_analysis_status = LogAnalysisStatus.FAILED

        service, agent, _notifier, pending = build(behaviour=failed_but_closed_ok)

        await service.on_slowlog(SlowlogEntry(timestamp=TS))
        await settle(service)

        assert len(agent.incidents) == 1, "실패한 실행이 재트리거를 걸었다"
        assert not pending.empty(), "큐가 비어 재트리거 조건 자체가 서지 않았다"

    async def test_분석이_성공했고_큐에_남은_것이_있으면_이어서_한_번_더_돈다(self):
        """대조군. 위 테스트가 "재트리거가 아예 불가능해서" 통과하는 것이
        아님을 보인다 — 같은 조건에서 성공하면 실제로 한 번 더 돈다."""

        def succeeded(run_index, state, pending):
            if run_index == 1:
                pending.put(SlowlogEntry(timestamp=TS))
            state.latest_analysis_status = LogAnalysisStatus.COMPLETED

        service, agent, _notifier, pending = build(behaviour=succeeded)

        await service.on_slowlog(SlowlogEntry(timestamp=TS))
        await settle(service)

        assert len(agent.incidents) == 2
        # 두 번째 실행의 유입 정착이 큐를 비웠으므로 거기서 멎는다.
        assert pending.empty()
