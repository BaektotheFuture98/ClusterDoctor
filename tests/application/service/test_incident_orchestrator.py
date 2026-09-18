"""Incident Lifecycle. Supervisor 사이클과 런타임 상한이 실제로 지켜지는가.

요구사항 1·2·3·9·10·11이 여기 있다.

Supervisor도 SubAgent도 대본대로 움직이는 가짜를 쓴다. 확인하려는 것은 모델의
판단이 아니라 **판단과 실행 사이의 배선** — 무엇이 거절되고, 무엇이 좁혀지고,
무엇이 다음 요청으로 이어지는가다.
"""

import asyncio
from datetime import datetime, timedelta

import pytest

from cluster_doctor.application.service.guardrails import (
    MAX_ANALYZED_MINUTES,
    window_minutes,
)
from cluster_doctor.application.service.incident_orchestrator import IncidentOrchestrator
from cluster_doctor.domain.model.evidence import Evidence, EvidenceSource
from cluster_doctor.domain.model.incident import Incident, IncidentStatus
from cluster_doctor.domain.model.log_analysis import (
    LogAnalysisResponse,
    LogAnalysisStatus,
    VerificationStatus,
)
from cluster_doctor.domain.model.log_analysis_report import LogAnalysisReport
from cluster_doctor.domain.model.supervisor_decision import (
    SupervisorAction,
    SupervisorDecision,
)
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.infrastructure.outbound.state.in_memory_artifact_store import (
    InMemoryArtifactStore,
)
from cluster_doctor.infrastructure.outbound.state.in_memory_incident_state_repository import (
    InMemoryIncidentStateRepository,
)
from tests.domain.model.test_time_range_spans import KST, label, span

TRIGGER = datetime(2026, 9, 18, 14, 3, tzinfo=KST)


def incident(incident_id: str = "inc-1") -> Incident:
    return Incident(
        incident_id=incident_id,
        cluster="es-prod",
        trigger_time=TRIGGER,
        kafka_receive_time=TRIGGER + timedelta(seconds=5),
    )


def request_analysis(window: TimeRange, goal: str = "테스트") -> SupervisorDecision:
    return SupervisorDecision(
        action=SupervisorAction.REQUEST_ANALYSIS,
        analysis_window=window,
        analysis_goal=goal,
        reason="테스트 대본",
    )


COMPLETE = SupervisorDecision(
    action=SupervisorAction.COMPLETE_INCIDENT, reason="충분하다"
)


class ScriptedSupervisor:
    """대본대로 판단한다. 대본이 떨어지면 종료를 고른다."""

    def __init__(self, *decisions: SupervisorDecision) -> None:
        self._decisions = list(decisions)
        self.calls: list[dict] = []

    def decide(self, incident, state, *, last_response=None, candidate_windows=()):
        self.calls.append(
            {
                "incident_id": incident.incident_id,
                "analyzed": list(state.analyzed_windows),
                "analysis_call_count": state.analysis_call_count,
                "candidates": list(candidate_windows),
                "last_status": last_response.status if last_response else None,
                "latest_report_ref": state.latest_report_ref,
            }
        )
        if not self._decisions:
            return COMPLETE
        return self._decisions.pop(0)


class ScriptedAgent:
    """대본대로 응답한다. 리포트는 실제로 저장소에 넣는다."""

    def __init__(self, store, *responses) -> None:
        self._store = store
        self._responses = list(responses)
        self.requests: list = []

    def analyze(self, request):
        self.requests.append(request)
        report_ref = self._store.put_report(
            request.incident_id,
            LogAnalysisReport(
                incident_id=request.incident_id,
                analyzed_from=request.analysis_window.start,
                analyzed_to=request.analysis_window.end,
                summary=f"{len(self.requests)}번째 분석",
            ),
        )
        template = (
            self._responses.pop(0)
            if self._responses
            else LogAnalysisResponse(
                status=LogAnalysisStatus.COMPLETED,
                analyzed_window=request.analysis_window,
            )
        )
        return template.model_copy(
            update={
                "analyzed_window": request.analysis_window,
                "report_ref": report_ref,
            }
        )


class RecordingNotifier:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def notify(self, report, *, gaps=(), analysis_failed=False):
        self.calls.append(
            {"report": report, "gaps": gaps, "analysis_failed": analysis_failed}
        )


def build(supervisor, responses, *, store=None, notifier=None):
    store = store or InMemoryArtifactStore()
    notifier = notifier or RecordingNotifier()
    agent = ScriptedAgent(store, *responses)
    orchestrator = IncidentOrchestrator(
        supervisor=supervisor,
        log_analysis_agent=agent,
        state_repository=InMemoryIncidentStateRepository(),
        artifact_store=store,
        notifier=notifier,
        drain_pending=lambda: [],
        # 유입 정착 대기를 0으로 둔다. 기다림 자체는 test_guardrails가 본다.
        wait_step_seconds=0,
    )
    return orchestrator, agent, store, notifier


def analyzed_windows(agent: ScriptedAgent) -> list[tuple[str, str]]:
    return label(request.analysis_window for request in agent.requests)


class TestScopeExpansion:
    async def test_SubAgent가_앞_구간을_요구하면_새_요청이_생긴다(self):
        """요구사항 1번.

        14:00~14:10을 분석한 뒤 "그 전부터 징후가 있었다"는 답이 오면
        Supervisor가 13:50~14:00에 대한 새 요청을 만든다.
        """
        supervisor = ScriptedSupervisor(
            request_analysis(span(14, 0, 14, 10)),
            request_analysis(span(13, 50, 14, 0), "JVM pressure 시작 시점 확인"),
            COMPLETE,
        )
        orchestrator, agent, *_ = build(
            supervisor,
            [
                LogAnalysisResponse(
                    status=LogAnalysisStatus.NEED_MORE_CONTEXT,
                    analyzed_window=span(14, 0, 14, 10),
                    suggested_windows=(span(13, 50, 14, 0),),
                )
            ],
        )

        await orchestrator.run(incident())

        assert analyzed_windows(agent) == [
            ("14:00", "14:10"),
            ("13:50", "14:00"),
        ]
        assert agent.requests[1].analysis_goal == "JVM pressure 시작 시점 확인"

    async def test_제안_구간이_다음_사이클의_후보로_전달된다(self):
        """차집합은 코드가 계산해 Supervisor에게 건넨다. 모델이 산수를 하지
        않는다는 것이 이 설계의 경계다."""
        supervisor = ScriptedSupervisor(
            request_analysis(span(14, 0, 14, 10)), COMPLETE
        )
        orchestrator, *_ = build(
            supervisor,
            [
                LogAnalysisResponse(
                    status=LogAnalysisStatus.NEED_MORE_CONTEXT,
                    analyzed_window=span(14, 0, 14, 10),
                    suggested_windows=(span(13, 50, 14, 0),),
                )
            ],
        )

        await orchestrator.run(incident())

        assert ("13:50", "14:00") in label(supervisor.calls[1]["candidates"])

    async def test_이미_분석한_구간은_후보에서_빠진다(self):
        supervisor = ScriptedSupervisor(request_analysis(span(14, 0, 14, 10)), COMPLETE)
        orchestrator, *_ = build(
            supervisor,
            [
                LogAnalysisResponse(
                    status=LogAnalysisStatus.NEED_MORE_CONTEXT,
                    analyzed_window=span(14, 0, 14, 10),
                    suggested_windows=(span(14, 0, 14, 10),),
                )
            ],
        )

        await orchestrator.run(incident())

        assert ("14:00", "14:10") not in label(supervisor.calls[1]["candidates"])


class TestDuplicateGuardrail:
    async def test_같은_구간을_다시_요청하면_SubAgent를_부르지_않는다(self):
        """요구사항 2번. 프롬프트가 아니라 런타임이 막는다."""
        supervisor = ScriptedSupervisor(
            request_analysis(span(14, 0, 14, 10)),
            request_analysis(span(14, 0, 14, 10)),
            COMPLETE,
        )
        orchestrator, agent, *_ = build(supervisor, [])

        await orchestrator.run(incident())

        assert analyzed_windows(agent) == [("14:00", "14:10")]

    async def test_부분_중복은_아직_보지_않은_부분만_분석한다(self):
        """요구사항 3번.

        요청을 통째로 거절하지 않는다. 거절하면 모델이 똑같은 요청을 다시
        내놓고 사이클만 태운다.
        """
        supervisor = ScriptedSupervisor(
            request_analysis(span(14, 0, 14, 10)),
            request_analysis(span(13, 55, 14, 5)),
            COMPLETE,
        )
        orchestrator, agent, *_ = build(supervisor, [])

        await orchestrator.run(incident())

        assert analyzed_windows(agent) == [
            ("14:00", "14:10"),
            ("13:55", "14:00"),
        ]

    async def test_거절이_반복되면_Incident를_닫는다(self):
        """모델이 상한을 이해하지 못하고 같은 요청을 되풀이할 때의 안전장치."""
        supervisor = ScriptedSupervisor(
            request_analysis(span(14, 0, 14, 10)),
            *[request_analysis(span(14, 0, 14, 10)) for _ in range(8)],
        )
        orchestrator, agent, *_ = build(supervisor, [])

        outcome = await orchestrator.run(incident())

        assert len(agent.requests) == 1
        assert outcome.status is IncidentStatus.COMPLETED


class TestAnalysisBudget:
    async def test_NEED_MORE_CONTEXT가_반복돼도_분_예산을_넘지_않는다(self):
        """요구사항 9번.

        예산 단위가 호출 수가 아니라 **분**이다. 10분짜리 창 여섯 개면 60분
        예산을 정확히 다 쓴다.
        """
        windows = [
            span(13, 0, 13, 10),
            span(13, 10, 13, 20),
            span(13, 20, 13, 30),
            span(13, 30, 13, 40),
            span(13, 40, 13, 50),
            span(13, 50, 14, 0),
            span(14, 0, 14, 10),
            span(14, 10, 14, 20),
            span(14, 20, 14, 30),
            span(14, 30, 14, 40),
        ]
        supervisor = ScriptedSupervisor(*[request_analysis(w) for w in windows])
        always_more = [
            LogAnalysisResponse(
                status=LogAnalysisStatus.NEED_MORE_CONTEXT,
                analyzed_window=window,
            )
            for window in windows
        ]
        orchestrator, agent, *_ = build(supervisor, always_more)

        await orchestrator.run(incident())

        analyzed = sum(window_minutes(r.analysis_window) for r in agent.requests)
        assert analyzed == MAX_ANALYZED_MINUTES

    async def test_짧은_구간은_예산을_조금만_쓴다(self):
        """호출 수로 세면 1분 창과 10분 창이 같은 예산을 먹는다. 그러면
        1분짜리 gap 하나를 메우는 데 예산 1/6이 날아간다."""
        windows = [span(13, 0, 13, 1), span(13, 10, 13, 11), span(14, 0, 14, 10)]
        supervisor = ScriptedSupervisor(
            *[request_analysis(w) for w in windows], COMPLETE
        )
        orchestrator, agent, *_ = build(supervisor, [])

        await orchestrator.run(incident())

        assert len(agent.requests) == 3
        assert sum(window_minutes(r.analysis_window) for r in agent.requests) == 12

    async def test_남은_예산보다_긴_요청은_앞쪽만_분석한다(self):
        """통째로 거절하면 남은 예산을 쓰지 못한 채 Incident가 끝난다.
        앞쪽을 남기는 것은 Supervisor가 시작 시각을 의도해서 고르기 때문이다."""
        filler = [
            span(13, 0, 13, 10),
            span(13, 10, 13, 20),
            span(13, 20, 13, 30),
            span(13, 30, 13, 40),
            span(13, 40, 13, 50),
        ]
        supervisor = ScriptedSupervisor(
            *[request_analysis(w) for w in filler],
            request_analysis(span(14, 0, 14, 10)),
            COMPLETE,
        )
        orchestrator, agent, *_ = build(supervisor, [])

        await orchestrator.run(incident())

        # 50분을 쓴 뒤 10분을 요청했으므로 남은 10분이 그대로 통과한다.
        assert analyzed_windows(agent)[-1] == ("14:00", "14:10")

    async def test_예산이_모자라면_그만큼만_잘라_분석한다(self):
        filler = [
            span(13, 0, 13, 10),
            span(13, 10, 13, 20),
            span(13, 20, 13, 30),
            span(13, 30, 13, 40),
            span(13, 40, 13, 50),
            span(13, 50, 13, 56),
        ]
        supervisor = ScriptedSupervisor(
            *[request_analysis(w) for w in filler],
            request_analysis(span(14, 0, 14, 10)),
            COMPLETE,
        )
        orchestrator, agent, *_ = build(supervisor, [])

        await orchestrator.run(incident())

        # 56분을 썼으므로 4분만 남는다.
        assert analyzed_windows(agent)[-1] == ("14:00", "14:04")


class TestStateNotContext:
    async def test_새_Incident는_이전_Incident의_상태를_잇지_않는다(self):
        """요구사항 10번.

        Agent Context는 버려도 되지만 Incident State는 남는다 — 그리고 그
        State는 Incident마다 따로다. 같은 orchestrator로 두 번 돌려도 두 번째
        Supervisor는 빈 상태에서 시작해야 한다.
        """
        store = InMemoryArtifactStore()
        supervisor = ScriptedSupervisor(
            request_analysis(span(14, 0, 14, 10)),
            COMPLETE,
            request_analysis(span(14, 0, 14, 10)),
            COMPLETE,
        )
        orchestrator, agent, *_ = build(supervisor, [], store=store)

        await orchestrator.run(incident("inc-1"))
        first_cycle_count = len(supervisor.calls)
        await orchestrator.run(incident("inc-2"))

        second_start = supervisor.calls[first_cycle_count]
        assert second_start["incident_id"] == "inc-2"
        assert second_start["analyzed"] == []
        assert second_start["analysis_call_count"] == 0
        assert second_start["latest_report_ref"] is None
        # 같은 구간을 다시 분석한다 — 앞 Incident의 중복 판정이 새 Incident로
        # 새어 나가지 않는다.
        assert analyzed_windows(agent) == [("14:00", "14:10"), ("14:00", "14:10")]

    async def test_Evidence_저장소도_Incident별로_나뉜다(self):
        store = InMemoryArtifactStore()
        store.put_raw("inc-1", "첫 Incident의 원문")
        store.put_raw("inc-2", "둘째 Incident의 원문")

        assert store.list_evidence("inc-1") == []
        assert store.get_observations("inc-1") is not store.get_observations("inc-2")

    async def test_같은_Incident의_앞선_리포트는_state_ref로_전달된다(self):
        """요구사항 11번.

        두 번째 분석은 첫 번째 리포트를 참조로 받는다. 전문을 실어 나르지
        않으면서도 이어서 판단할 수 있게 하는 것이 참조의 목적이다.
        """
        supervisor = ScriptedSupervisor(
            request_analysis(span(14, 0, 14, 10)),
            request_analysis(span(13, 50, 14, 0)),
            COMPLETE,
        )
        orchestrator, agent, store, _ = build(supervisor, [])

        await orchestrator.run(incident())

        first_ref = agent.requests[0].analysis_window and store.get_report(
            supervisor.calls[1]["latest_report_ref"]
        )
        assert agent.requests[0].state_ref is None
        assert agent.requests[1].state_ref == supervisor.calls[1]["latest_report_ref"]
        assert first_ref is not None
        assert first_ref.summary == "1번째 분석"


class TestDelivery:
    async def test_리포트는_항상_전달된다(self):
        supervisor = ScriptedSupervisor(COMPLETE)
        notifier = RecordingNotifier()
        orchestrator, *_ = build(supervisor, [], notifier=notifier)

        await orchestrator.run(incident())

        assert len(notifier.calls) == 1

    async def test_분석이_실패하면_배너가_붙는다(self):
        supervisor = ScriptedSupervisor(request_analysis(span(14, 0, 14, 10)), COMPLETE)
        notifier = RecordingNotifier()
        orchestrator, *_ = build(
            supervisor,
            [
                LogAnalysisResponse(
                    status=LogAnalysisStatus.FAILED,
                    analyzed_window=span(14, 0, 14, 10),
                    gaps=("조회 실패",),
                )
            ],
            notifier=notifier,
        )

        outcome = await orchestrator.run(incident())

        assert outcome.analysis_failed is True
        assert notifier.calls[0]["analysis_failed"] is True
        assert "조회 실패" in notifier.calls[0]["gaps"]

    async def test_검증_불일치도_재트리거를_막는다(self):
        """리포트는 존재하지만 근거와 맞지 않는다. 운영자가 그것을 모른 채
        다음 분석이 이어지면 틀린 리포트가 누적된다."""
        supervisor = ScriptedSupervisor(request_analysis(span(14, 0, 14, 10)), COMPLETE)
        orchestrator, *_ = build(
            supervisor,
            [
                LogAnalysisResponse(
                    status=LogAnalysisStatus.VALIDATION_FAILED,
                    analyzed_window=span(14, 0, 14, 10),
                    verification_status=VerificationStatus.MISMATCH,
                )
            ],
        )

        outcome = await orchestrator.run(incident())

        assert outcome.analysis_failed is True

    async def test_전달_실패가_Incident를_죽이지_않는다(self):
        class BrokenNotifier:
            async def notify(self, report, *, gaps=(), analysis_failed=False):
                raise RuntimeError("디스크 가득 참")

        supervisor = ScriptedSupervisor(COMPLETE)
        orchestrator, *_ = build(supervisor, [], notifier=BrokenNotifier())

        outcome = await orchestrator.run(incident())

        assert outcome.status is IncidentStatus.COMPLETED


class TestTermination:
    async def test_Supervisor가_종료를_고르면_거기서_끝난다(self):
        supervisor = ScriptedSupervisor(COMPLETE, request_analysis(span(14, 0, 14, 10)))
        orchestrator, agent, *_ = build(supervisor, [])

        outcome = await orchestrator.run(incident())

        assert agent.requests == []
        assert outcome.status is IncidentStatus.COMPLETED

    async def test_실패로_닫으면_그_사실이_남는다(self):
        supervisor = ScriptedSupervisor(
            SupervisorDecision(
                action=SupervisorAction.FAIL_INCIDENT, reason="근거를 얻지 못했다"
            )
        )
        orchestrator, *_ = build(supervisor, [])

        outcome = await orchestrator.run(incident())

        assert outcome.status is IncidentStatus.FAILED
        assert outcome.reason == "근거를 얻지 못했다"
        assert outcome.analysis_failed is True

    async def test_취소되면_취소로_닫는다(self):
        from cluster_doctor.application.service.guardrails import CancellationToken

        token = CancellationToken()
        token.cancel("운영자 중단")
        supervisor = ScriptedSupervisor(request_analysis(span(14, 0, 14, 10)))
        orchestrator, agent, *_ = build(supervisor, [])

        outcome = await orchestrator.run(incident(), cancellation=token)

        assert outcome.status is IncidentStatus.CANCELLED
        assert agent.requests == []


@pytest.mark.parametrize("window", [span(14, 0, 14, 10), span(13, 0, 13, 5)])
async def test_분석이_끝난_구간은_State에_남는다(window):
    supervisor = ScriptedSupervisor(request_analysis(window), COMPLETE)
    repository = InMemoryIncidentStateRepository()
    store = InMemoryArtifactStore()
    orchestrator = IncidentOrchestrator(
        supervisor=supervisor,
        log_analysis_agent=ScriptedAgent(store),
        state_repository=repository,
        artifact_store=store,
        notifier=RecordingNotifier(),
        drain_pending=lambda: [],
        wait_step_seconds=0,
    )

    await orchestrator.run(incident("inc-x"))

    state = repository.get("inc-x")
    assert window in state.analyzed_windows
    assert state.analysis_call_count == 1


class BudgetSupervisor:
    """후보가 있으면 첫 번째를 요청하고, 없으면 종료한다.

    대본이 아니라 상태를 보고 판단하므로 여러 Incident가 동시에 돌아도
    서로의 순서에 영향을 주지 않는다 — 동시성 테스트에는 이쪽이 필요하다.
    """

    def __init__(self) -> None:
        self.seen: dict[str, list[dict]] = {}

    def decide(self, incident, state, *, last_response=None, candidate_windows=()):
        self.seen.setdefault(incident.incident_id, []).append(
            {
                "analyzed": label(state.analyzed_windows),
                "analyzed_minutes": state.analyzed_minutes,
                "evidence_refs": list(state.evidence_refs),
            }
        )
        if not candidate_windows:
            return COMPLETE
        return request_analysis(candidate_windows[0])


class PerIncidentAgent:
    """Incident별로 요청을 기록하고 그 Incident의 Evidence 참조를 돌려준다."""

    def __init__(self, store) -> None:
        self._store = store
        self.requests: dict[str, list] = {}

    def analyze(self, request):
        self.requests.setdefault(request.incident_id, []).append(request)
        ref = self._store.next_evidence_id(request.incident_id)
        self._store.put_evidence(
            request.incident_id,
            Evidence(
                evidence_id=ref,
                event_time=request.analysis_window.start,
                source=EvidenceSource.SLOWLOG,
                message=f"{request.incident_id} 근거",
            ),
        )
        report_ref = self._store.put_report(
            request.incident_id,
            LogAnalysisReport(
                incident_id=request.incident_id,
                analyzed_from=request.analysis_window.start,
                analyzed_to=request.analysis_window.end,
                summary=f"{request.incident_id} 분석",
            ),
        )
        return LogAnalysisResponse(
            status=LogAnalysisStatus.COMPLETED,
            analyzed_window=request.analysis_window,
            report_ref=report_ref,
            evidence_refs=(ref,),
        )


class TestConcurrentIncidents:
    """Incident 여러 건이 겹쳐 돌아도 서로의 상태를 보지 않는다.

    저장소는 ``RLock``으로 read-modify-write를 막지만, 락이 있다는 것과
    **경로 전체가 격리된다**는 것은 다르다. 유입이 몰리면 실제로 겹쳐 돈다.
    """

    async def _run_many(self, count: int):
        store = InMemoryArtifactStore()
        supervisor = BudgetSupervisor()
        agent = PerIncidentAgent(store)
        orchestrator = IncidentOrchestrator(
            supervisor=supervisor,
            log_analysis_agent=agent,
            state_repository=InMemoryIncidentStateRepository(),
            artifact_store=store,
            notifier=RecordingNotifier(),
            drain_pending=lambda: [],
            wait_step_seconds=0,
        )
        incidents = [
            Incident(
                incident_id=f"inc-{index}",
                cluster="es-prod",
                # 서로 다른 시각을 준다. 초기 구간이 달라야 섞였는지 알 수 있다.
                trigger_time=TRIGGER + timedelta(hours=index),
                kafka_receive_time=TRIGGER + timedelta(hours=index, seconds=5),
            )
            for index in range(count)
        ]

        outcomes = await asyncio.gather(
            *(orchestrator.run(item) for item in incidents)
        )
        return supervisor, agent, store, outcomes

    async def test_전부_정상_종료한다(self):
        _supervisor, _agent, _store, outcomes = await self._run_many(5)

        assert [o.status for o in outcomes] == [IncidentStatus.COMPLETED] * 5

    async def test_각_Incident는_자기_구간만_분석한다(self):
        _supervisor, agent, _store, _outcomes = await self._run_many(5)

        for index in range(5):
            windows = agent.requests[f"inc-{index}"]
            expected_hour = (TRIGGER + timedelta(hours=index)).hour
            assert windows, f"inc-{index}가 아무것도 분석하지 않았다"
            assert all(
                w.analysis_window.start.hour in (expected_hour, expected_hour - 1)
                for w in windows
            ), f"inc-{index}에 다른 Incident의 구간이 섞였다"

    async def test_Supervisor가_다른_Incident의_State를_보지_않는다(self):
        supervisor, _agent, _store, _outcomes = await self._run_many(5)

        for incident_id, calls in supervisor.seen.items():
            assert calls[0]["analyzed"] == [], f"{incident_id}가 빈 상태로 시작하지 않았다"
            assert calls[0]["analyzed_minutes"] == 0
            assert calls[0]["evidence_refs"] == []

    async def test_Evidence가_Incident를_넘나들지_않는다(self):
        _supervisor, agent, store, _outcomes = await self._run_many(5)

        for index in range(5):
            incident_id = f"inc-{index}"
            stored = store.list_evidence(incident_id)

            assert len(stored) == len(agent.requests[incident_id])
            assert all(item.message.startswith(incident_id) for item in stored)

    async def test_예산도_Incident별로_따로_센다(self):
        _supervisor, _agent, _store, outcomes = await self._run_many(5)

        # 한 Incident가 다른 Incident의 예산을 먹으면 뒤쪽이 분석을 못 한다.
        assert all(o.analysis_calls >= 1 for o in outcomes)
