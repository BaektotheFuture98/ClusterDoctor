from datetime import datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.health_point import (
    HealthPoint,
    fold_health,
    is_green,
)
from cluster_doctor.incident_analysis_agent.model.kst import KST

_T = datetime(2026, 11, 13, 10, 0, tzinfo=KST)


def test_same_state_extends_until():
    first = fold_health((), {"status": "red", "unassigned_shards": 3}, _T)
    later = _T + timedelta(seconds=30)
    folded = fold_health(first, {"status": "red", "unassigned_shards": 3}, later)
    assert folded == (HealthPoint(at=_T, until=later, status="red", unassigned_shards=3),)


def test_changed_state_appends_point():
    first = fold_health((), {"status": "red", "unassigned_shards": 3}, _T)
    later = _T + timedelta(seconds=30)
    folded = fold_health(first, {"status": "yellow", "unassigned_shards": 1}, later)
    assert [point.status for point in folded] == ["red", "yellow"]
    assert folded[1].at == later


def test_is_green_ignores_case():
    assert is_green({"status": "GREEN"})
    assert not is_green({"status": "yellow"})
    assert not is_green({})
