import pytest
from datetime import datetime
from cluster_doctor.domain.model.time_range import TimeRange

FROM = datetime(2026, 8, 20, 2, 9, 0)
TO   = datetime(2026, 8, 20, 2, 10, 0)

def test_creates_valid_time_range():
    tr = TimeRange(start=FROM, end=TO)
    assert tr.start == FROM
    assert tr.end == TO

def test_raises_when_start_equals_end():
    with pytest.raises(ValueError, match="이전"):
        TimeRange(start=FROM, end=FROM)

def test_raises_when_start_after_end():
    with pytest.raises(ValueError, match="이전"):
        TimeRange(start=TO, end=FROM)

def test_raises_when_start_is_none():
    with pytest.raises((ValueError, TypeError)):
        TimeRange(start=None, end=TO)

def test_raises_when_end_is_none():
    with pytest.raises((ValueError, TypeError)):
        TimeRange(start=FROM, end=None)
