"""Incident 수명주기. 유입 정착 → Agent → 종료 확정 → 전달.

``IncidentRunner``에는 더 이상 분석 루프가 없다. 구간 선택도 위임도 예산도
Agent 안으로 들어갔고, 여기 남은 것은 **Agent가 협조하지 않아도 성립해야 하는
것**뿐이다 — 벽시계 상한, 취소, 그리고 "리포트는 항상 전달된다".

그래서 이 파일의 Agent는 전부 가짜다. 확인하려는 것은 Agent가 무엇을 판단하는지가
아니라 **그 판단이 무엇이든 수명주기가 같은 모양으로 끝나는가**이다.
"""

import asyncio
import threading
from datetime import datetime, timedelta

import pytest

from cluster_doctor.application.port.outbound.incident_agent import IncidentAgentResult
from cluster_doctor.application.service.guardrails import CancellationToken
from cluster_doctor.application.service.incident_runner import IncidentRunner
from cluster_doctor.contracts.observations import Observations, TimelineRow
from cluster_doctor.contracts.evidence import Evidence, EvidenceSource
from cluster_doctor.domain.model.incident import Incident, IncidentStatus
from cluster_doctor.agent.contracts import (
    LogAnalysisStatus,
    VerificationStatus,
)
from cluster_doctor.contracts.report import LogAnalysisReport
from cluster_doctor.storage.in_memory_artifact_store import (
    InMemoryArtifactStore,
)
from cluster_doctor.storage.in_memory_incident_state_store import (
    InMemoryIncidentStateRepository,
)
from tests.contracts.test_time_range_spans import KST, span

TRIGGER = datetime(2026, 9, 18, 14, 3, tzinfo=KST)


def incident(incident_id: str = "inc-1") -> Incident:
    return Incident(
        incident_id=incident_id,
        cluster="es-prod",
        trigger_time=TRIGGER,
        kafka_receive_time=TRIGGER + timedelta(seconds=5),
    )


class FakeIncidentAgent:
    """``IncidentAgent`` 포트를 만족하는 가짜.

    실제 구현과 같은 계약을 지킨다 — ``state``를 제자리에서 갱신하고 저장소에도
    반영한다. 그 계약이 깨지면 Runner가 종료 뒤에 State를 다시 읽는 이유가
    사라지므로, 가짜도 그렇게 움직여야 테스트가 의미를 갖는다.
    """

    def __init__(self, repository, *, behaviour=None, result=None) -> None:
        self._repository = repository
        self._behaviour = behaviour
        self._result = result or IncidentAgentResult(status=IncidentStatus.COMPLETED)
        self.calls: list[tuple[Incident, object]] = []
        self.seen_pending: list[list] = []
        # 호출 시점의 스냅샷. ``state``는 살아 있는 객체라 나중에 읽으면
        # Agent가 자기 손으로 바꾼 값을 보게 되고, "빈 상태로 시작했는가"를
        # 물을 수 없다.
        self.seen_at_entry: list[dict] = []

    def run(self, incident, state):
        self.calls.append((incident, state))
        self.seen_pending.append(list(state.pending_windows))
        self.seen_at_entry.append(
            {
                "incident_id": state.incident_id,
                "analyzed": list(state.analyzed_windows),
                "analysis_call_count": state.analysis_call_count,
                "analyzed_minutes": state.analyzed_minutes,
                "latest_report_ref": state.latest_report_ref,
            }
        )
        if self._behaviour is not None:
            return self._behaviour(incident, state, self._repository) or self._result
        self._repository.save(state)
        return self._result


class RecordingNotifier:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def notify(self, report, *, gaps=(), analysis_failed=False):
        self.calls.append(
            {"report": report, "gaps": gaps, "analysis_failed": analysis_failed}
        )


def build(
    *,
    agent=None,
    behaviour=None,
    result=None,
    store=None,
    notifier=None,
    drain_pending=None,
    incident_timeout_seconds=30.0,
):
    store = store or InMemoryArtifactStore()
    notifier = notifier or RecordingNotifier()
    repository = InMemoryIncidentStateRepository()
    agent = agent or FakeIncidentAgent(repository, behaviour=behaviour, result=result)
    runner = IncidentRunner(
        incident_agent=agent,
        state_repository=repository,
        artifact_store=store,
        notifier=notifier,
        drain_pending=drain_pending or (lambda: []),
        incident_timeout_seconds=incident_timeout_seconds,
        # 유입 정착 대기를 0으로 둔다. 기다림의 예산 자체는 test_guardrails가 본다.
        wait_step_seconds=0,
    )
    return runner, agent, store, notifier, repository


def report_for(incident_id: str, window) -> LogAnalysisReport:
    return LogAnalysisReport(
        incident_id=incident_id,
        analyzed_from=window.start,
        analyzed_to=window.end,
        summary="테스트 리포트",
    )


class TestLifecycle:
    async def test_유입이_정착하면_Agent에게_넘긴다(self):
        """요구사항 1번의 착지점.

        Kafka 이벤트로 열린 Incident가 실제로 Agent 실행까지 간다. 중간에
        루프가 없으므로 이 경로가 끊기면 아무것도 분석되지 않는다.
        """
        runner, agent, *_ = build()

        outcome = await runner.run(incident())

        assert len(agent.calls) == 1
        assert agent.calls[0][0].incident_id == "inc-1"
        assert outcome.status is IncidentStatus.COMPLETED

    async def test_초기_구간이_Agent가_보기_전에_State에_들어간다(self):
        """Agent는 빈 State를 받지 않는다.

        트리거 시각에서 만든 초기 구간이 ``pending_windows``에 없으면
        ``list_candidate_windows``가 빈 목록을 돌려주고, 모델은 볼 구간을
        산문에서 지어내게 된다.
        """
        runner, agent, *_ = build()

        await runner.run(incident())

        assert agent.seen_pending[0], "초기 후보 구간 없이 Agent가 불렸다"

    async def test_Agent가_닫지_않고_끝나도_Incident는_닫힌다(self):
        """종료를 선언하지 않은 채 끝나는 것은 Agent의 흔한 실패다.

        그대로 두면 status가 ANALYZING으로 남아 운영자에게는 영원히 진행
        중인 Incident가 된다.
        """

        def no_close(_incident, _state, _repository):
            return IncidentAgentResult(status=IncidentStatus.ANALYZING)

        runner, *_ = build(behaviour=no_close)

        outcome = await runner.run(incident())

        assert outcome.status is IncidentStatus.COMPLETED

    async def test_Agent가_이미_닫았으면_그_판단을_덮지_않는다(self):
        def close_as_failed(_incident, state, repository):
            state.status = IncidentStatus.FAILED
            state.closing_reason = "근거를 하나도 얻지 못했다"
            repository.save(state)
            return IncidentAgentResult(
                status=IncidentStatus.FAILED, reason=state.closing_reason, failed=True
            )

        runner, *_ = build(behaviour=close_as_failed)

        outcome = await runner.run(incident())

        assert outcome.status is IncidentStatus.FAILED
        assert outcome.reason == "근거를 하나도 얻지 못했다"
        assert outcome.analysis_failed is True

    async def test_위임_없이_끝나도_정상_종료다(self):
        """요구사항 3번.

        볼 구간이 없다고 판단해 한 번도 위임하지 않는 것은 실패가 아니다.
        실패로 기록하면 리포트에 붉은 배너가 붙고 재트리거까지 막힌다.
        """
        runner, _agent, _store, notifier, repository = build()

        outcome = await runner.run(incident())

        assert outcome.analysis_calls == 0
        assert outcome.analysis_failed is False
        assert repository.get("inc-1").analysis_call_count == 0
        assert len(notifier.calls) == 1

    async def test_새_Incident는_앞선_Incident의_State를_잇지_않는다(self):
        def burn_budget(_incident, state, repository):
            state.analysis_call_count += 1
            state.analyzed_minutes += 10
            state.record_analyzed(span(14, 0, 14, 10))
            repository.save(state)
            return IncidentAgentResult(status=IncidentStatus.COMPLETED)

        runner, agent, *_ = build(behaviour=burn_budget)

        await runner.run(incident("inc-1"))
        await runner.run(incident("inc-2"))

        second = agent.seen_at_entry[1]
        assert second["incident_id"] == "inc-2"
        assert second["analyzed"] == []
        assert second["analysis_call_count"] == 0
        assert second["analyzed_minutes"] == 0
        assert second["latest_report_ref"] is None


class TestWallClock:
    async def test_상한을_넘기면_기다리기를_그만두고_전달한다(self):
        """요구사항 14번.

        대기·조회·LLM이 각자 상한을 가져도 그 곱은 묶이지 않는다. 여기서
        끊지 않으면 Incident 하나가 프로세스를 몇 시간 점유한다.
        """
        release = threading.Event()

        def hang(_incident, _state, _repository):
            release.wait(5)
            return IncidentAgentResult(status=IncidentStatus.COMPLETED)

        runner, _agent, _store, notifier, _repository = build(
            behaviour=hang, incident_timeout_seconds=0.05
        )
        try:
            outcome = await runner.run(incident())
        finally:
            release.set()

        assert outcome.status is IncidentStatus.FAILED
        assert "상한" in outcome.reason
        assert outcome.analysis_failed is True
        # 상한을 넘겼다고 리포트를 버리지 않는다.
        assert len(notifier.calls) == 1

    async def test_취소되면_Agent를_부르지도_않는다(self):
        token = CancellationToken()
        token.cancel("운영자 중단")
        runner, agent, *_ = build()

        outcome = await runner.run(incident(), cancellation=token)

        assert outcome.status is IncidentStatus.CANCELLED
        assert agent.calls == []

    async def test_Agent_실행_중에도_이벤트_루프가_돌아간다(self):
        """요구사항 11번.

        ``IncidentAgent.run``은 동기이고 LLM 왕복이 수 분 걸린다. 같은 루프가
        Kafka를 소비하므로 여기서 막으면 그동안 도착한 slowlog가 쌓이기만
        한다 — ``asyncio.to_thread``로 밀어내는 이유다.

        루프가 막히지 않았다는 것을 "함께 돌린 코루틴이 실제로 전진했는가"로
        본다. 시간으로 재면 느린 CI에서 흔들린다.
        """
        started = threading.Event()
        release = threading.Event()
        ticks = 0

        def block(_incident, _state, _repository):
            started.set()
            release.wait(5)
            return IncidentAgentResult(status=IncidentStatus.COMPLETED)

        async def tick():
            nonlocal ticks
            # Agent가 스레드로 넘어갈 때까지 기다린 뒤, 루프가 살아 있는지 센다.
            while not started.is_set():
                await asyncio.sleep(0.01)
            for _ in range(5):
                ticks += 1
                await asyncio.sleep(0.01)
            release.set()

        runner, *_ = build(behaviour=block)

        outcome, _ = await asyncio.gather(runner.run(incident()), tick())

        assert ticks == 5, "Agent가 실행되는 동안 이벤트 루프가 막혔다"
        assert outcome.status is IncidentStatus.COMPLETED


class TestDelivery:
    async def test_리포트는_항상_전달된다(self):
        """요구사항 13번."""
        runner, _agent, _store, notifier, _repository = build()

        await runner.run(incident())

        assert len(notifier.calls) == 1

    async def test_분석이_실패해도_관측값은_전달된다(self):
        """요구사항 12번.

        코드가 모은 관측값은 모델이 전부 실패해도 남는다. 여기서 리포트를
        버리면 운영자는 그 시각에 무슨 일이 있었는지조차 알 수 없다.
        """
        store = InMemoryArtifactStore()
        store.merge_observations(
            "inc-1",
            Observations(
                timeline=(TimelineRow(minute=TRIGGER, counts={"slowlog": 3}),),
                first_seen=TRIGGER,
                last_seen=TRIGGER,
            ),
        )

        def fail(_incident, state, repository):
            state.status = IncidentStatus.FAILED
            state.closing_reason = "조회가 전부 실패했다"
            repository.save(state)
            return IncidentAgentResult(
                status=IncidentStatus.FAILED,
                reason=state.closing_reason,
                failed=True,
                gaps=("ClickHouse 조회 실패",),
            )

        runner, _agent, _store, notifier, _repository = build(
            behaviour=fail, store=store
        )

        outcome = await runner.run(incident())

        assert outcome.analysis_failed is True
        call = notifier.calls[0]
        assert call["analysis_failed"] is True
        assert call["report"].observations.timeline, "관측값이 전달되지 않았다"
        assert "ClickHouse 조회 실패" in call["gaps"]

    async def test_종료_사유는_배너에_함께_실린다(self):
        def fail(_incident, state, repository):
            state.status = IncidentStatus.FAILED
            state.closing_reason = "예산을 다 쓴 채 근거가 없었다"
            repository.save(state)
            return IncidentAgentResult(status=IncidentStatus.FAILED, failed=True)

        runner, _agent, _store, notifier, _repository = build(behaviour=fail)

        await runner.run(incident())

        assert any(
            "예산을 다 쓴 채 근거가 없었다" in gap for gap in notifier.calls[0]["gaps"]
        )

    async def test_Agent가_쌓아_둔_gap이_전달된다(self):
        """``accumulated_gaps``는 Incident 전체에 걸쳐 쌓인다.

        위임마다 덮어쓰면 마지막 한 번의 누락만 남고, 리포트가 실제보다
        완전해 보인다.
        """

        def accumulate(_incident, state, repository):
            state.accumulated_gaps.extend(["노드 로그 접속 실패", "마스터 로그 누락"])
            repository.save(state)
            return IncidentAgentResult(status=IncidentStatus.COMPLETED)

        runner, _agent, _store, notifier, _repository = build(behaviour=accumulate)

        outcome = await runner.run(incident())

        assert set(outcome.gaps) == {"노드 로그 접속 실패", "마스터 로그 누락"}
        assert "노드 로그 접속 실패" in notifier.calls[0]["gaps"]

    async def test_전달_실패가_Incident를_죽이지_않는다(self):
        class BrokenNotifier:
            async def notify(self, report, *, gaps=(), analysis_failed=False):
                raise RuntimeError("디스크 가득 참")

        runner, *_ = build(notifier=BrokenNotifier())

        outcome = await runner.run(incident())

        assert outcome.status is IncidentStatus.COMPLETED


class TestVerification:
    async def test_검증_불일치가_최종_결과까지_간다(self):
        """요구사항 9번.

        리포트는 있지만 근거와 맞지 않는다. 운영자가 그것을 모른 채 재트리거가
        이어지면 틀린 리포트가 누적된다.
        """

        def mismatch(_incident, state, repository):
            state.latest_analysis_status = LogAnalysisStatus.VALIDATION_FAILED
            state.latest_verification_status = VerificationStatus.MISMATCH
            repository.save(state)
            return IncidentAgentResult(status=IncidentStatus.COMPLETED)

        runner, _agent, _store, notifier, _repository = build(behaviour=mismatch)

        outcome = await runner.run(incident())

        # Agent는 COMPLETED로 닫았지만 재트리거는 막혀야 한다.
        assert outcome.status is IncidentStatus.COMPLETED
        assert outcome.analysis_failed is True
        # 배너(``analysis_failed``)는 **분석이 깨졌을 때** 붙는다. 검증 불일치는
        # 리포트가 성립하되 근거와 어긋난 것이라, 운영자에게는 gap 문장으로
        # 닿고 재트리거만 막는다. 둘을 같은 신호로 묶으면 쓸 수 있는 리포트에
        # 붉은 배너가 붙어 멀쩡한 내용까지 의심하게 된다.
        assert notifier.calls[0]["analysis_failed"] is False

    async def test_리포트의_검증_지적이_배너에_실린다(self):
        store = InMemoryArtifactStore()
        window = span(14, 0, 14, 10)
        report = report_for("inc-1", window).model_copy(
            update={"verification_issues": ("근거 E-1이 리포트에 없다",)}
        )
        ref = store.put_report("inc-1", report)

        def attach(_incident, state, repository):
            state.latest_report_ref = ref
            repository.save(state)
            return IncidentAgentResult(status=IncidentStatus.COMPLETED)

        runner, _agent, _store, notifier, _repository = build(
            behaviour=attach, store=store
        )

        await runner.run(incident())

        assert any("근거 E-1이 리포트에 없다" in gap for gap in notifier.calls[0]["gaps"])


class TestAgentContractBreach:
    async def test_Agent가_예외를_올려도_리포트는_나간다(self):
        """포트 계약은 "예외를 올리지 않는다"이지만, 계약이 깨져도 코드가
        모아 둔 관측값까지 사라지면 안 된다."""
        store = InMemoryArtifactStore()
        store.merge_observations(
            "inc-1", Observations(timeline=(TimelineRow(minute=TRIGGER, counts={"slowlog": 1}),))
        )

        def explode(_incident, _state, _repository):
            raise RuntimeError("deepagents 내부에서 터졌다")

        runner, _agent, _store, notifier, _repository = build(
            behaviour=explode, store=store
        )

        outcome = await runner.run(incident())

        assert outcome.status is IncidentStatus.FAILED
        assert outcome.analysis_failed is True
        assert notifier.calls[0]["report"].observations.timeline

    async def test_같은_예외가_반복돼도_재시도하지_않는다(self):
        """요구사항 10번의 한 갈래.

        Runner는 Agent를 **한 번만** 부른다. 여기에 재시도가 있으면 429로
        실패한 호출이 백오프 없이 증폭된다.
        """
        attempts = 0

        def explode(_incident, _state, _repository):
            nonlocal attempts
            attempts += 1
            raise RuntimeError("429 quota exceeded")

        runner, *_ = build(behaviour=explode)

        await runner.run(incident())

        assert attempts == 1


@pytest.mark.parametrize("window", [span(14, 0, 14, 10), span(13, 0, 13, 5)])
async def test_Agent가_기록한_분석_구간이_State에_남는다(window):
    def analyze(_incident, state, repository):
        state.analysis_call_count += 1
        state.record_analyzed(window)
        evidence_id = "E-1"
        repository.save(state)
        return IncidentAgentResult(status=IncidentStatus.COMPLETED, reason=evidence_id)

    runner, _agent, _store, _notifier, repository = build(behaviour=analyze)

    await runner.run(incident("inc-x"))

    state = repository.get("inc-x")
    assert window in state.analyzed_windows
    assert state.analysis_call_count == 1


class TestConcurrentIncidents:
    """Incident 여러 건이 겹쳐 돌아도 서로의 상태를 보지 않는다."""

    async def _run_many(self, count: int):
        store = InMemoryArtifactStore()
        repository = InMemoryIncidentStateRepository()

        def analyze(item, state, repo):
            ref = store.next_evidence_id(item.incident_id)
            store.put_evidence(
                item.incident_id,
                Evidence(
                    evidence_id=ref,
                    event_time=item.trigger_time,
                    source=EvidenceSource.SLOWLOG,
                    message=f"{item.incident_id} 근거",
                ),
            )
            state.analysis_call_count += 1
            state.evidence_refs.append(ref)
            repo.save(state)
            return IncidentAgentResult(status=IncidentStatus.COMPLETED)

        agent = FakeIncidentAgent(repository, behaviour=analyze)
        runner = IncidentRunner(
            incident_agent=agent,
            state_repository=repository,
            artifact_store=store,
            notifier=RecordingNotifier(),
            drain_pending=lambda: [],
            wait_step_seconds=0,
        )
        incidents = [
            Incident(
                incident_id=f"inc-{index}",
                cluster="es-prod",
                trigger_time=TRIGGER + timedelta(hours=index),
                kafka_receive_time=TRIGGER + timedelta(hours=index, seconds=5),
            )
            for index in range(count)
        ]
        outcomes = await asyncio.gather(*(runner.run(item) for item in incidents))
        return store, repository, outcomes

    async def test_전부_정상_종료한다(self):
        _store, _repository, outcomes = await self._run_many(5)

        assert [o.status for o in outcomes] == [IncidentStatus.COMPLETED] * 5

    async def test_Evidence가_Incident를_넘나들지_않는다(self):
        store, _repository, _outcomes = await self._run_many(5)

        for index in range(5):
            incident_id = f"inc-{index}"
            stored = store.list_evidence(incident_id)
            assert len(stored) == 1
            assert stored[0].message.startswith(incident_id)

    async def test_예산도_Incident별로_따로_센다(self):
        _store, repository, outcomes = await self._run_many(5)

        assert all(o.analysis_calls == 1 for o in outcomes)
        assert all(repository.get(f"inc-{i}").analysis_call_count == 1 for i in range(5))


class RacingStateRepository(InMemoryIncidentStateRepository):
    """좀비 스레드의 뒤늦은 쓰기를 **결정적으로** 재현한다.

    타임아웃으로 기다리기를 끊어도 Agent 스레드는 계속 돈다 — 파이썬은 남의
    스레드를 죽이지 못한다. 그 스레드가 비종료 상태를 저장하는 순간과 Runner가
    저장소를 다시 읽는 순간의 순서는 실제로는 정해져 있지 않고, 시간으로
    맞추려 들면 테스트가 흔들린다.

    그래서 순서를 저장소 쪽에서 만든다. ``arm()`` 이후 첫 ``get``이 **읽기
    직전에** 비종료 상태를 써 넣는다. Runner가 타임아웃 뒤 처음 읽는 값이
    정확히 그 좀비 쓰기가 된다.
    """

    def __init__(self) -> None:
        super().__init__()
        self._armed = False
        self.zombie_writes = 0

    def arm(self) -> None:
        self._armed = True

    def get(self, incident_id: str):
        if self._armed:
            self._armed = False
            zombie = super().get(incident_id)
            zombie.status = IncidentStatus.ANALYZING
            zombie.closing_reason = "좀비 스레드가 덮어쓴 값"
            super().save(zombie)
            self.zombie_writes += 1
        return super().get(incident_id)


class TestForcedTerminationWins:
    """Runner가 직접 내린 종료는 저장소 재조회에 지지 않는다.

    타임아웃과 취소는 **모델이 협조하든 말든 성립해야 하는 사실**이다. 확정한
    뒤 저장소를 다시 읽어 그 값으로 덮으면, 뒤늦게 깨어난 Agent 스레드가 써 둔
    비종료 상태 때문에 FAILED로 닫은 Incident가 COMPLETED로 뒤집힌다 —
    ``analysis_failed``는 True인 채로. 리포트에는 실패 배너가 붙는데 상태는
    완료라고 적히고, 운영자는 둘 중 어느 쪽을 믿을지 알 수 없다.
    """

    async def test_좀비_스레드가_비종료_상태를_덮어써도_타임아웃은_FAILED로_닫힌다(self):
        release = threading.Event()
        repository = RacingStateRepository()

        def hang(_incident, _state, _repo):
            release.wait(5)
            return IncidentAgentResult(status=IncidentStatus.COMPLETED)

        agent = FakeIncidentAgent(repository, behaviour=hang)
        runner = IncidentRunner(
            incident_agent=agent,
            state_repository=repository,
            artifact_store=InMemoryArtifactStore(),
            notifier=RecordingNotifier(),
            drain_pending=lambda: [],
            incident_timeout_seconds=0.05,
            wait_step_seconds=0,
        )

        # ``run``이 저장소를 처음 **읽는** 곳이 타임아웃 직후의 재조회다.
        # 여기서 무장해 두면 그 읽기가 곧 좀비 쓰기를 본 읽기가 된다 — 스레드
        # 기동 속도에 기대지 않으므로 느린 CI에서도 흔들리지 않는다.
        repository.arm()

        try:
            outcome = await runner.run(incident())
        finally:
            release.set()

        assert repository.zombie_writes == 1, "좀비 쓰기가 재현되지 않았다"
        assert outcome.status is IncidentStatus.FAILED
        assert outcome.analysis_failed is True
        assert "상한" in outcome.reason
        # 저장소에도 확정된 종료가 남아야 한다. 뒤에 읽는 쪽이 ANALYZING을
        # 보면 끝난 Incident가 진행 중으로 보인다.
        assert repository.get("inc-1").status is IncidentStatus.FAILED

    async def test_좀비_스레드가_덮어써도_예외_종료는_FAILED로_닫힌다(self):
        repository = RacingStateRepository()

        def explode(_incident, _state, _repo):
            raise RuntimeError("deepagents 내부에서 터졌다")

        agent = FakeIncidentAgent(repository, behaviour=explode)
        runner = IncidentRunner(
            incident_agent=agent,
            state_repository=repository,
            artifact_store=InMemoryArtifactStore(),
            notifier=RecordingNotifier(),
            drain_pending=lambda: [],
            wait_step_seconds=0,
        )

        repository.arm()

        outcome = await runner.run(incident())

        assert repository.zombie_writes == 1
        assert outcome.status is IncidentStatus.FAILED
        assert outcome.analysis_failed is True

    async def test_취소도_저장소_재조회에_지지_않는다(self):
        repository = RacingStateRepository()
        token = CancellationToken()
        token.cancel("운영자 중단")
        agent = FakeIncidentAgent(repository)
        runner = IncidentRunner(
            incident_agent=agent,
            state_repository=repository,
            artifact_store=InMemoryArtifactStore(),
            notifier=RecordingNotifier(),
            drain_pending=lambda: [],
            wait_step_seconds=0,
        )
        repository.arm()

        outcome = await runner.run(incident(), cancellation=token)

        assert outcome.status is IncidentStatus.CANCELLED
        assert repository.get("inc-1").status is IncidentStatus.CANCELLED


class TestFailedDelegationsBlockRetrigger:
    """모델의 종료 선언보다 **실제로 무슨 일이 일어났는가**가 앞선다.

    위임이 전부 FAILED로 돌아왔는데 모델이 ``finish_incident(COMPLETED)``를
    부르면, 예전에는 그것이 성공으로 기록되어 같은 망가진 데이터 경로를 향해
    재트리거가 최대 3회 더 걸렸다. 모델의 문장이 회계를 정하게 두면 안 된다.
    """

    async def test_모든_위임이_실패하면_모델이_완료로_닫아도_실패로_센다(self):
        def failed_analysis(_incident, state, repository):
            state.latest_analysis_status = LogAnalysisStatus.FAILED
            state.status = IncidentStatus.COMPLETED
            state.closing_reason = "다 봤다"
            repository.save(state)
            # Agent 자신은 실패를 보고하지 않는다 — 모델이 완료로 닫았기 때문이다.
            return IncidentAgentResult(
                status=IncidentStatus.COMPLETED, reason="다 봤다", failed=False
            )

        runner, *_ = build(behaviour=failed_analysis)

        outcome = await runner.run(incident())

        assert outcome.status is IncidentStatus.COMPLETED
        assert outcome.analysis_failed is True

    async def test_분석이_성공했으면_완료가_완료로_남는다(self):
        """상한이 아니라 사실을 본다. 성공한 분석까지 실패로 적으면 재트리거가
        영영 걸리지 않고, 큐에 남은 slowlog가 다음 사고까지 방치된다."""

        def ok_analysis(_incident, state, repository):
            state.latest_analysis_status = LogAnalysisStatus.COMPLETED
            repository.save(state)
            return IncidentAgentResult(status=IncidentStatus.COMPLETED)

        runner, *_ = build(behaviour=ok_analysis)

        outcome = await runner.run(incident())

        assert outcome.analysis_failed is False
