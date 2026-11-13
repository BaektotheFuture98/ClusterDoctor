from datetime import datetime, timedelta

import pytest

from cluster_doctor.exceptions import GuardrailViolation
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.guardrails import (
    MAX_ANALYZED_MINUTES,
    admit_window,
    remaining_minutes,
)
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.window_planner import (
    continuation_windows,
    plan_new_windows,
)

_T = datetime(2026, 11, 13, 10, 0, tzinfo=KST)
_W = TimeRange(_T, _T + timedelta(minutes=10))


def test_prior_windows_are_not_proposed_again():
    assert plan_new_windows([_W], {"analyzed_windows": (), "prior_windows": (_W,)}) == []


def test_prior_windows_are_rejected_by_admission():
    with pytest.raises(GuardrailViolation):
        admit_window(_W, {"prior_windows": (_W,)})


def test_prior_windows_do_not_spend_budget():
    assert remaining_minutes({"prior_windows": (_W,)}) == MAX_ANALYZED_MINUTES


def test_continuation_windows_start_at_incident_start():
    start = _T + timedelta(seconds=40)
    assert continuation_windows(start, start + timedelta(minutes=20)) == [
        TimeRange(_T, _T + timedelta(minutes=10))
    ]


def test_continuation_windows_empty_for_empty_range():
    assert continuation_windows(_T, _T) == []
