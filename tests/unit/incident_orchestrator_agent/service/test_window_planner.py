from datetime import datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.basemodel.kst import KST
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.window_planner import (
    initial_windows,
)


def _kst(*args) -> datetime:
    return datetime(*args, tzinfo=KST)


def test_short_span_becomes_single_window_covering_it():
    first_seen = _kst(2026, 1, 1, 13, 50, 0)
    last_seen = _kst(2026, 1, 1, 13, 54, 0)

    windows = initial_windows(first_seen, last_seen)

    assert len(windows) == 1
    assert windows[0].start == _kst(2026, 1, 1, 13, 50, 0)
    assert windows[0].end == _kst(2026, 1, 1, 13, 55, 0)


def test_long_span_is_clamped_to_ten_minutes_from_the_start():
    first_seen = _kst(2026, 1, 1, 13, 50, 0)
    last_seen = _kst(2026, 1, 1, 14, 30, 0)

    windows = initial_windows(first_seen, last_seen)

    assert len(windows) == 1
    assert windows[0].start == _kst(2026, 1, 1, 13, 50, 0)
    assert windows[0].end == _kst(2026, 1, 1, 14, 0, 0)
    assert windows[0].end - windows[0].start == timedelta(minutes=10)


def test_first_seen_off_minute_boundary_only_floors_down():
    first_seen = _kst(2026, 1, 1, 13, 50, 37)
    last_seen = _kst(2026, 1, 1, 13, 52, 10)

    windows = initial_windows(first_seen, last_seen)

    assert windows[0].start == _kst(2026, 1, 1, 13, 50, 0)
    assert windows[0].end == _kst(2026, 1, 1, 13, 53, 0)


def test_first_seen_equal_to_last_seen_gives_minimum_one_minute_window():
    moment = _kst(2026, 1, 1, 13, 50, 0)

    windows = initial_windows(moment, moment)

    assert windows[0].start == _kst(2026, 1, 1, 13, 50, 0)
    assert windows[0].end == _kst(2026, 1, 1, 13, 51, 0)


def test_last_seen_before_first_seen_still_yields_a_valid_window():
    first_seen = _kst(2026, 1, 1, 13, 50, 0)
    last_seen = _kst(2026, 1, 1, 13, 49, 59)

    windows = initial_windows(first_seen, last_seen)

    assert windows[0].start == _kst(2026, 1, 1, 13, 50, 0)
    assert windows[0].end == _kst(2026, 1, 1, 13, 51, 0)
