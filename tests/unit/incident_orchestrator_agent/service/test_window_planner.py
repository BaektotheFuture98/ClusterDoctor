from datetime import datetime

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_orchestrator_agent.agent.prompts.system_prompt import SYSTEM_PROMPT
from cluster_doctor.incident_orchestrator_agent.service.analysis_window.window_planner import (
    initial_windows,
)


def _kst(*args) -> datetime:
    return datetime(*args, tzinfo=KST)


def test_first_window_runs_from_five_minutes_before_to_the_end_of_the_first_minute():
    windows = initial_windows(_kst(2026, 1, 1, 13, 50, 0))

    assert len(windows) == 1
    assert windows[0].start == _kst(2026, 1, 1, 13, 45, 0)
    assert windows[0].end == _kst(2026, 1, 1, 13, 51, 0)


def test_first_seen_off_minute_boundary_only_floors_down():
    windows = initial_windows(_kst(2026, 1, 1, 13, 50, 37))

    assert windows[0].start == _kst(2026, 1, 1, 13, 45, 0)
    assert windows[0].end == _kst(2026, 1, 1, 13, 51, 0)


def test_system_prompt_example_matches_initial_windows():
    window = initial_windows(_kst(2026, 1, 1, 14, 0, 37))[0]
    expected = f"{window.start:%H:%M}~{window.end:%H:%M}"

    assert expected == "13:55~14:01"
    assert expected in SYSTEM_PROMPT
