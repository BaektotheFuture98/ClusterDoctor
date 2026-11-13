import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisReport,
    VerificationStatus,
)
from cluster_doctor.incident_orchestrator_agent.model.incident import IncidentStatus
from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentAnalysisDetails,
    IncidentClose,
    IncidentOutcome,
)
from cluster_doctor.kafka_consumer.trigger_settling.service.inflow import (
    InflowTracker,
    ProblemLogSignal,
)
from cluster_doctor.kafka_consumer.trigger_settling.service.problem_log_processor import (
    ProblemLogProcessor,
    _Arrival,
)


class _Analysis:
    def __init__(self, on_handle=None, status=None, fail_first=False):
        self.commands = []
        self.on_handle = on_handle
        self.status = status
        self.fail_first = fail_first

    async def handle(self, command):
        self.commands.append(command)
        if self.on_handle:
            await self.on_handle(len(self.commands))
        if self.fail_first and len(self.commands) == 1:
            raise RuntimeError("analysis down")
        start = command.observed_start
        report = None
        if self.status is not None:
            report = LogAnalysisReport(
                incident_id=command.incident.incident_id,
                analyzed_from=start,
                analyzed_to=start + timedelta(minutes=1),
                verification_status=self.status,
            )
        return IncidentOutcome(
            incident_id=command.incident.incident_id,
            status=IncidentStatus.COMPLETED,
            report=report,
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


def test_span_cap_does_not_drain_queued_signal():
    processor = _processor(_Analysis(), _health("red"), span=timedelta(milliseconds=50))
    start = datetime.now(UTC) - timedelta(hours=1)
    processor._pending.put_nowait(
        _Arrival(ProblemLogSignal(timestamp=start), received_at=start)
    )
    tracker = InflowTracker(first_seen=start, last_seen=start, time_basis="log_timestamp")
    built = asyncio.run(processor._build_incident(tracker, start))
    assert built.close is IncidentClose.SPAN_LIMIT
    assert built.end == start + timedelta(milliseconds=50)
    assert not processor._pending.empty()


def test_span_cap_with_green_closes_as_green_and_keeps_signal():
    processor = _processor(_Analysis(), _health("green"), span=timedelta(milliseconds=50))
    start = datetime.now(UTC) - timedelta(hours=1)
    processor._pending.put_nowait(
        _Arrival(ProblemLogSignal(timestamp=start), received_at=start)
    )
    tracker = InflowTracker(first_seen=start, last_seen=start, time_basis="log_timestamp")
    built = asyncio.run(processor._build_incident(tracker, start))
    assert built.close is IncidentClose.GREEN
    assert not processor._pending.empty()


def test_signal_queued_at_span_cap_creates_next_incident():
    processor = None

    async def slow_with_signal(count):
        if count == 1:
            await processor.submit(ProblemLogSignal(timestamp=datetime.now(UTC)))
            await asyncio.sleep(0.2)

    analysis = _Analysis(on_handle=slow_with_signal)
    processor = _processor(analysis, _health("green"), span=timedelta(milliseconds=50))
    asyncio.run(_run(processor))
    assert [c.session.sequence for c in analysis.commands[:3]] == [1, 2, 3]


@pytest.mark.parametrize(
    "status", [VerificationStatus.MISMATCH, VerificationStatus.UNCHECKED]
)
def test_failed_verification_report_is_not_forwarded(status):
    analysis = _Analysis(status=status)
    asyncio.run(_run(_processor(analysis, _health("red"))))
    assert len(analysis.commands) == 3
    assert all(c.session.previous_report is None for c in analysis.commands)


def test_passed_report_is_forwarded():
    analysis = _Analysis(status=VerificationStatus.PASSED)
    asyncio.run(_run(_processor(analysis, _health("red"))))
    second = analysis.commands[1]
    assert second.session.previous_report is not None
    assert second.session.previous_report.incident_id == analysis.commands[0].incident.incident_id


def test_session_onset_is_first_incident_start():
    analysis = _Analysis()
    asyncio.run(_run(_processor(analysis, _health("red"))))
    assert all(c.session.onset == analysis.commands[0].observed_start for c in analysis.commands)


def test_no_health_probe_after_last_allowed_incident():
    analysis = _Analysis()
    processor = _processor(analysis, _health("red"))
    calls = 0
    original = processor._probe_green

    async def counting():
        nonlocal calls
        calls += 1
        return await original()

    processor._probe_green = counting
    asyncio.run(_run(processor))
    assert len(analysis.commands) == 3
    assert calls == 2


def test_close_while_forming_incident_returns_promptly():
    checked = asyncio.Event()

    async def probe():
        checked.set()
        return {"status": "red", "unassigned_shards": 0}

    analysis = _Analysis()
    processor = _processor(analysis, probe, span=timedelta(hours=1))

    async def scenario():
        await processor.submit(ProblemLogSignal(timestamp=datetime.now(UTC)))
        await asyncio.wait_for(checked.wait(), 5)
        await asyncio.wait_for(processor.close(), 2)

    asyncio.run(scenario())
    assert analysis.commands == []


def test_close_waits_for_running_analysis_and_starts_no_next_incident():
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocking(count):
        entered.set()
        await release.wait()

    analysis = _Analysis(on_handle=blocking)
    processor = _processor(analysis, _health("red"))

    async def scenario():
        await processor.submit(ProblemLogSignal(timestamp=datetime.now(UTC)))
        await asyncio.wait_for(entered.wait(), 5)
        closing = asyncio.create_task(processor.close())
        await asyncio.sleep(0.1)
        assert not closing.done()
        release.set()
        await asyncio.wait_for(closing, 2)

    asyncio.run(scenario())
    assert len(analysis.commands) == 1


def test_failed_analysis_continues_session_without_previous_report():
    analysis = _Analysis(fail_first=True)
    asyncio.run(_run(_processor(analysis, _health("red"))))
    first, second = analysis.commands[:2]
    assert second.session.previous_report is None
    assert second.session.previous_windows == ()
    assert second.session.previous_incident_id == first.incident.incident_id
