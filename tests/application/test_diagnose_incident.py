import asyncio
import threading
from datetime import UTC, datetime

from cluster_doctor.application.commands import StartIncident
from cluster_doctor.application.ports.incident_analyzer import IncidentAnalysisResult
from cluster_doctor.application.ports.report_publisher import ReportPublication
from cluster_doctor.application.use_cases.diagnose_incident import DiagnoseIncident
from cluster_doctor.domain.diagnosis.report import LogAnalysisReport, VerificationStatus
from cluster_doctor.domain.diagnosis.observations import Observations
from cluster_doctor.domain.diagnosis.evidence import Evidence, EvidenceSource
from cluster_doctor.domain.incident.guardrails import CancellationToken
from cluster_doctor.domain.incident.models import Incident, IncidentStatus, TriggerType
from cluster_doctor.adapters.outbound.persistence.in_memory_artifact_store import InMemoryArtifactStore
from cluster_doctor.adapters.outbound.persistence.in_memory_incident_state_store import (
    InMemoryIncidentStateRepository,
)

TS = datetime(2026, 9, 26, 10, 0, tzinfo=UTC)


def command(*, settling_wait_seconds: float = 0.01) -> StartIncident:
    incident = Incident(
        incident_id="inc-1",
        cluster="es-prod",
        trigger_time=TS,
        kafka_receive_time=TS,
        trigger_type=TriggerType.SLOWLOG,
    )
    return StartIncident(incident, TS, TS, settling_wait_seconds=settling_wait_seconds)


def command_for(incident_id: str) -> StartIncident:
    base = command()
    return StartIncident(
        Incident(
            incident_id=incident_id, cluster=base.incident.cluster,
            trigger_time=TS, kafka_receive_time=TS, trigger_type=TriggerType.SLOWLOG,
        ),
        TS, TS, 0,
    )


class RecordingAnalyzer:
    def __init__(self, repository, *, behaviour=None) -> None:
        self.repository = repository
        self.behaviour = behaviour
        self.incidents: list[Incident] = []

    def analyze(self, incident: Incident) -> IncidentAnalysisResult:
        self.incidents.append(incident)
        state = self.repository.get(incident.incident_id)
        assert state.pending_windows
        if self.behaviour is not None:
            return self.behaviour(incident)
        return IncidentAnalysisResult(IncidentStatus.COMPLETED)


class RecordingReportPublisher:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def publish(self, report, *, gaps=(), analysis_failed=False) -> ReportPublication:
        self.calls.append((report, gaps, analysis_failed))
        return ReportPublication(text_length=123)


class RacingStateRepository(InMemoryIncidentStateRepository):
    """Returns one stale ANALYZING snapshot at the post-decision reload."""

    def __init__(self) -> None:
        super().__init__()
        self._inject_stale = False

    def create(self, state):
        super().create(state)
        self._inject_stale = True

    def get(self, incident_id):
        state = super().get(incident_id)
        if self._inject_stale:
            self._inject_stale = False
            state.status = IncidentStatus.ANALYZING
            state.closing_reason = "stale"
        return state


def diagnosis_for(analyzer, repository, notifier, **kwargs) -> DiagnoseIncident:
    return DiagnoseIncident(
        incident_analyzer=analyzer,
        state_repository=repository,
        artifact_store=InMemoryArtifactStore(),
        report_publisher=notifier,
        **kwargs,
    )


async def test_정착된_명령으로_상태를_만들고_analyzer는_id로_상태를_읽는다():
    repository = InMemoryIncidentStateRepository()
    analyzer = RecordingAnalyzer(repository)
    notifier = RecordingReportPublisher()

    outcome = await diagnosis_for(analyzer, repository, notifier).handle(command())

    assert analyzer.incidents == [command().incident]
    assert outcome.status is IncidentStatus.COMPLETED
    assert repository.get("inc-1").total_wait_seconds == 0.01
    assert len(notifier.calls) == 1


async def test_analyzer_예외도_FAILED로_닫고_최종_리포트를_전달한다():
    repository = InMemoryIncidentStateRepository()

    def explode(_incident):
        raise RuntimeError("agent exploded")

    notifier = RecordingReportPublisher()
    analyzer = RecordingAnalyzer(repository, behaviour=explode)
    outcome = await diagnosis_for(
        analyzer, repository, notifier
    ).handle(command())

    assert outcome.status is IncidentStatus.FAILED
    assert outcome.analysis_failed is True
    assert "Agent" in outcome.reason
    assert len(notifier.calls) == 1
    assert notifier.calls[0][2] is True
    assert len(analyzer.incidents) == 1
    assert outcome.diagnostics.observations == Observations()
    assert outcome.diagnostics.evidence == ()


async def test_미확정_분석_리포트는_전달_전에_최종_리포트로_병합된다():
    repository = InMemoryIncidentStateRepository()
    store = InMemoryArtifactStore()
    ref = store.put_report(
        "inc-1",
        LogAnalysisReport(
            incident_id="inc-1",
            analyzed_from=TS,
            analyzed_to=TS,
            summary="window report",
        ),
    )

    def leave_window_report(_incident):
        state = repository.get("inc-1")
        state.report_refs.append(ref)
        repository.save(state)
        return IncidentAnalysisResult(IncidentStatus.COMPLETED)

    notifier = RecordingReportPublisher()
    outcome = await DiagnoseIncident(
        incident_analyzer=RecordingAnalyzer(repository, behaviour=leave_window_report),
        state_repository=repository,
        artifact_store=store,
        report_publisher=notifier,
    ).handle(command())

    assert outcome.report_ref is not None
    assert outcome.report_ref != ref
    assert "window report" in notifier.calls[0][0].narrative.headline


async def test_시간초과는_마지막_저장상태를_FAILED로_강제_종료하고_전달한다():
    repository = InMemoryIncidentStateRepository()
    release = threading.Event()

    def hang(_incident):
        release.wait(1)
        return IncidentAnalysisResult(IncidentStatus.COMPLETED)

    notifier = RecordingReportPublisher()
    try:
        outcome = await diagnosis_for(
            RecordingAnalyzer(repository, behaviour=hang),
            repository,
            notifier,
            incident_timeout_seconds=0.01,
        ).handle(command())
    finally:
        release.set()

    assert outcome.status is IncidentStatus.FAILED
    assert outcome.analysis_failed is True
    assert "상한" in outcome.reason
    assert len(notifier.calls) == 1


async def test_timeout_forced_closure_wins_over_stale_state():
    repository = RacingStateRepository()
    release = threading.Event()

    class BlockingAnalyzer:
        def analyze(self, _incident):
            release.wait(1)
            return IncidentAnalysisResult(IncidentStatus.COMPLETED)

    try:
        outcome = await diagnosis_for(
            BlockingAnalyzer(), repository, RecordingReportPublisher(),
            incident_timeout_seconds=0.01,
        ).handle(command())
    finally:
        release.set()

    assert outcome.status is IncidentStatus.FAILED
    assert repository.get("inc-1").status is IncidentStatus.FAILED


async def test_cancellation_forced_closure_wins_over_stale_state():
    repository = RacingStateRepository()
    token = CancellationToken()
    token.cancel("operator")

    outcome = await diagnosis_for(
        RecordingAnalyzer(repository), repository, RecordingReportPublisher()
    ).handle(command(), cancellation=token)

    assert outcome.status is IncidentStatus.CANCELLED
    assert repository.get("inc-1").status is IncidentStatus.CANCELLED


async def test_analyzer_runs_off_the_event_loop_thread():
    repository = InMemoryIncidentStateRepository()
    seen = []
    event_loop_thread = threading.get_ident()

    def record_thread(_incident):
        seen.append(threading.get_ident())
        return IncidentAnalysisResult(IncidentStatus.COMPLETED)

    await diagnosis_for(
        RecordingAnalyzer(repository, behaviour=record_thread),
        repository,
        RecordingReportPublisher(),
    ).handle(command())

    assert seen[0] != event_loop_thread


async def test_report_publisher_failure_does_not_fail_incident():
    class BrokenPublisher:
        async def publish(self, *_args, **_kwargs):
            raise OSError("disk full")

    repository = InMemoryIncidentStateRepository()
    outcome = await diagnosis_for(
        RecordingAnalyzer(repository), repository, BrokenPublisher()
    ).handle(command())

    assert outcome.status is IncidentStatus.COMPLETED


async def test_observations_and_accumulated_gaps_survive_analyzer_failure():
    repository = InMemoryIncidentStateRepository()
    store = InMemoryArtifactStore()

    def persist_then_fail(incident):
        state = repository.get(incident.incident_id)
        state.accumulated_gaps.append("SSH unavailable")
        repository.save(state)
        store.merge_observations(incident.incident_id, Observations(time_basis="observed"))
        raise RuntimeError("analysis failed")

    outcome = await DiagnoseIncident(
        incident_analyzer=RecordingAnalyzer(repository, behaviour=persist_then_fail),
        state_repository=repository,
        artifact_store=store,
        report_publisher=RecordingReportPublisher(),
    ).handle(command())

    assert outcome.gaps == ("SSH unavailable",)
    assert outcome.diagnostics.observations.time_basis == "observed"


async def test_verification_mismatch_marks_outcome_failed_without_rewriting_publisher_flag():
    repository = InMemoryIncidentStateRepository()
    publisher = RecordingReportPublisher()

    def mismatch(_incident):
        state = repository.get("inc-1")
        state.latest_verification_status = VerificationStatus.MISMATCH
        repository.save(state)
        return IncidentAnalysisResult(IncidentStatus.COMPLETED, failed=False)

    outcome = await diagnosis_for(
        RecordingAnalyzer(repository, behaviour=mismatch), repository, publisher
    ).handle(command())

    assert outcome.analysis_failed is True
    assert publisher.calls[0][2] is False


async def test_concurrent_incidents_keep_state_and_evidence_isolated():
    repository = InMemoryIncidentStateRepository()
    store = InMemoryArtifactStore()

    def record(incident):
        store.put_evidence(
            incident.incident_id,
            Evidence(
                evidence_id=f"E-{incident.incident_id}-1", event_time=TS,
                source=EvidenceSource.SLOWLOG, message=incident.incident_id,
            ),
        )
        return IncidentAnalysisResult(IncidentStatus.COMPLETED)

    use_case = DiagnoseIncident(
        incident_analyzer=RecordingAnalyzer(repository, behaviour=record),
        state_repository=repository, artifact_store=store,
        report_publisher=RecordingReportPublisher(),
    )
    left, right = await asyncio.gather(
        use_case.handle(command_for("left")), use_case.handle(command_for("right"))
    )

    assert [item.message for item in left.diagnostics.evidence] == ["left"]
    assert [item.message for item in right.diagnostics.evidence] == ["right"]


async def test_정착에_전체_시간_예산을_썼으면_analyzer를_부르지_않는다():
    repository = InMemoryIncidentStateRepository()
    analyzer = RecordingAnalyzer(repository)
    notifier = RecordingReportPublisher()

    outcome = await diagnosis_for(
        analyzer,
        repository,
        notifier,
        incident_timeout_seconds=0.01,
    ).handle(command(settling_wait_seconds=0.01))

    assert analyzer.incidents == []
    assert outcome.status is IncidentStatus.FAILED
    assert outcome.analysis_failed is True
    assert "상한" in outcome.reason


async def test_취소는_analyzer를_부르지_않고_CANCELLED로_닫고_전달한다():
    repository = InMemoryIncidentStateRepository()
    analyzer = RecordingAnalyzer(repository)
    notifier = RecordingReportPublisher()
    token = CancellationToken()
    token.cancel("운영자 중단")

    outcome = await diagnosis_for(analyzer, repository, notifier).handle(
        command(), cancellation=token
    )

    assert analyzer.incidents == []
    assert outcome.status is IncidentStatus.CANCELLED
    assert outcome.reason == "운영자 중단"
    assert len(notifier.calls) == 1
