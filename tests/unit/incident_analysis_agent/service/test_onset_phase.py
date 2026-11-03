import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import Observations, TimelineRow
from cluster_doctor.incident_analysis_agent.service.report_generation.analysis_context import (
    build_analysis_context,
)
from cluster_doctor.incident_analysis_agent.service.report_generation.onset_phase import onset_phase

ONSET = datetime(2026, 1, 1, 14, 0, 37, tzinfo=KST)


def _kst(hour, minute, second=0):
    return datetime(2026, 1, 1, hour, minute, second, tzinfo=KST)


def _evidence(evidence_id, at):
    return Evidence(
        evidence_id=evidence_id, event_time=at, source=EvidenceSource.SLOWLOG, message="m"
    )


def _query(at, run_time):
    return QueryLogEntry(
        reg_date=at, host="client", run_time=Decimal(run_time), success="1",
        s_date=0, e_date=0, date_range=0, keyword=(), target_host="es-1",
        index_name="idx", conditions=(), cmd="search", service="svc", env="prod",
        project="p", company="c", user="u", search_count=1, etc="", cluster="cl",
    )


@pytest.mark.parametrize(
    ("at", "phase"),
    [
        (_kst(13, 59, 59), "before"),
        (_kst(14, 0, 0), "onset"),
        (_kst(14, 0, 59), "onset"),
        (_kst(14, 1, 0), "after"),
    ],
    ids=["last-second-before-the-minute", "start-of-the-minute", "end-of-the-minute", "start-of-the-next-minute"],
)
def test_phase_boundaries_follow_the_onset_minute(at, phase):
    assert onset_phase(at, ONSET) == phase


def test_phase_does_not_depend_on_the_timezone_of_the_input():
    at = _kst(14, 0, 10).astimezone(timezone.utc)

    assert onset_phase(at, ONSET) == "onset"


def test_context_tags_each_evidence_with_its_phase():
    evidence = [
        _evidence("E-1", _kst(13, 58)),
        _evidence("E-2", _kst(14, 0, 50)),
        _evidence("E-3", _kst(14, 3)),
    ]

    data = json.loads(build_analysis_context(Observations(), evidence, onset=ONSET))

    assert data["onset_kst"] == ONSET.isoformat()
    assert [item["phase"] for item in data["evidence"]] == ["before", "onset", "after"]


def test_context_tags_minute_observations():
    observations = Observations(timeline=(
        TimelineRow(minute=_kst(13, 59)),
        TimelineRow(minute=_kst(14, 0)),
        TimelineRow(minute=_kst(14, 1)),
    ))

    data = json.loads(build_analysis_context(observations, [], onset=ONSET))

    assert [row["phase"] for row in data["minute_observations"]] == ["before", "onset", "after"]


def test_context_tags_slow_executions():
    observations = Observations(query_requests=(
        _query(_kst(14, 5), "30"),
        _query(_kst(13, 50), "20"),
        _query(_kst(14, 0, 40), "10"),
    ))

    data = json.loads(build_analysis_context(observations, [], onset=ONSET))

    assert [row["phase"] for row in data["slow_executions"]] == ["after", "before", "onset"]


def test_context_without_onset_has_no_phase():
    observations = Observations(
        timeline=(TimelineRow(minute=_kst(14, 0)),),
        query_requests=(_query(_kst(14, 0), "10"),),
    )

    data = json.loads(build_analysis_context(observations, [_evidence("E-1", _kst(14, 0))]))

    assert "onset_kst" not in data
    assert "phase" not in data["evidence"][0]
    assert "phase" not in data["minute_observations"][0]
    assert "phase" not in data["slow_executions"][0]
