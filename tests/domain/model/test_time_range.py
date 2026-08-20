import pytest
from datetime import datetime, timedelta
from cluster_doctor.domain.model.time_range import (
    MAX_TIME_RANGE_DURATION,
    InvalidTimeRangeError,
    TimeRange,
)

FROM = datetime(2026, 8, 20, 2, 9, 0)
TO   = datetime(2026, 8, 20, 2, 10, 0)

def test_creates_valid_time_range():
    tr = TimeRange(start=FROM, end=TO)
    assert tr.start == FROM
    assert tr.end == TO

def test_raises_when_start_equals_end():
    with pytest.raises(InvalidTimeRangeError, match="이전"):
        TimeRange(start=FROM, end=FROM)

def test_raises_when_start_after_end():
    with pytest.raises(InvalidTimeRangeError, match="이전"):
        TimeRange(start=TO, end=FROM)

def test_raises_when_start_is_none():
    with pytest.raises(InvalidTimeRangeError):
        TimeRange(start=None, end=TO)

def test_raises_when_end_is_none():
    with pytest.raises(InvalidTimeRangeError):
        TimeRange(start=FROM, end=None)

def test_invalid_time_range_error_is_a_value_error():
    # Kept intentionally: callers may still handle it value-style. The HTTP
    # layer must map only this type -- not bare ValueError -- to 400.
    assert issubclass(InvalidTimeRangeError, ValueError)


def test_accepts_range_exactly_at_the_one_hour_limit():
    tr = TimeRange(start=FROM, end=FROM + MAX_TIME_RANGE_DURATION)
    assert tr.end - tr.start == MAX_TIME_RANGE_DURATION


def test_rejects_range_longer_than_one_hour():
    with pytest.raises(InvalidTimeRangeError, match="1"):
        TimeRange(start=FROM, end=FROM + MAX_TIME_RANGE_DURATION + timedelta(seconds=1))


def test_internal_one_minute_segments_are_never_rejected_by_the_cap():
    # TimeRange also represents the one-minute segments the ClickHouse
    # adapter builds internally -- those must never trip the 1-hour cap.
    tr = TimeRange(start=FROM, end=FROM + timedelta(minutes=1))
    assert tr.end - tr.start == timedelta(minutes=1)
