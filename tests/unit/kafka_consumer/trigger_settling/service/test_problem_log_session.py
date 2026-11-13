import asyncio
from datetime import UTC, datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_orchestrator_agent.model.incident import IncidentStatus
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentAnalysisDetails,
    IncidentClose,
    IncidentOutcome,
)
from cluster_doctor.kafka_consumer.trigger_settling.service.inflow import ProblemLogSignal
from cluster_doctor.kafka_consumer.trigger_settling.service.problem_log_processor import (
    ProblemLogProcessor,
)


class _Analysis:
    def __init__(self, on_handle=None):
        self.commands = []
        self.on_handle = on_handle

    async def handle(self, command):
        self.commands.append(command)
        if self.on_handle:
            await self.on_handle(len(self.commands))
        start = command.observed_start
        return IncidentOutcome(
            incident_id=command.incident.incident_id,
            status=IncidentStatus.COMPLETED,
            diagnostics=IncidentAnalysisDetails(
                observations=Observations(requested=((start, start + timedelta(minutes=1)),))
            ),
        )


def _health(*statuses):
    remaining = list(statuses)

    async def probe():
        status = remaining.pop(0) if len(remaining) > 1 else remaining[0]
        if isinstance(status, Exception):
            raise status
        return {"status": status, "unassigned_shards": 0}

    return probe


def _processor(analysis, health=None, span=timedelta(milliseconds=60)):
    return ProblemLogProcessor(
        analyze_incident=analysis,
        cluster_health=health,
        quiet_period_seconds=0.005,
        max_settling_wait_seconds=0.05,
        incident_span_limit=span,
    )


async def _run(processor):
    await processor.submit(ProblemLogSignal(timestamp=datetime.now(UTC)))
    await processor._task


def test_green_makes_single_incident():
    analysis = _Analysis()
    asyncio.run(_run(_processor(analysis, _health("green"))))
    [command] = analysis.commands
    assert command.close is IncidentClose.GREEN
    assert command.session.sequence == 1
    assert command.health_history[0].status == "green"


def test_red_splits_into_session_of_three():
    analysis = _Analysis()
    asyncio.run(_run(_processor(analysis, _health("red"))))
    assert [c.session.sequence for c in analysis.commands] == [1, 2, 3]
    assert [c.session.last_allowed for c in analysis.commands] == [False, False, True]
    assert all(c.close is IncidentClose.SPAN_LIMIT for c in analysis.commands)
    first, second, third = analysis.commands
    assert second.observed_start == first.incident_end
    assert second.session.previous_incident_id == first.incident.incident_id
    assert len(third.session.previous_windows) == 2
    assert third.session.pseudonyms is first.session.pseudonyms
    assert third.session.onset == first.observed_start


def test_health_failure_closes_and_ends_session():
    analysis = _Analysis()
    asyncio.run(_run(_processor(analysis, _health(RuntimeError("down")))))
    [command] = analysis.commands
    assert command.close is IncidentClose.HEALTH_UNKNOWN
    assert command.health_gaps == ("health 확인 불가로 incident를 마감했다",)


def test_signal_during_analysis_continues_session_even_if_green():
    processor = None

    async def late_signal(count):
        if count == 1:
            await processor.submit(ProblemLogSignal(timestamp=datetime.now(UTC)))

    analysis = _Analysis(on_handle=late_signal)
    processor = _processor(analysis, _health("green"))
    asyncio.run(_run(processor))
    assert [c.session.sequence for c in analysis.commands] == [1, 2]


def test_without_probe_behaves_as_before():
    analysis = _Analysis()
    asyncio.run(_run(_processor(analysis, None)))
    [command] = analysis.commands
    assert command.incident_end is None
    assert command.session is None
    assert command.close is IncidentClose.NOT_WAITED
