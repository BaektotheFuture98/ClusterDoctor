import pytest
from datetime import datetime, timedelta, timezone
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

def test_invalid_time_range_error_remains_a_value_error_subclass():
    # This pins a compatibility contract, not an implementation detail:
    # callers written against the pre-split API catch ValueError, and the
    # base class is the only thing keeping them working. Narrowing
    # InvalidTimeRangeError to plain Exception would silently break them,
    # and nothing else in the suite would notice.
    assert issubclass(InvalidTimeRangeError, ValueError)


def test_cap_is_exactly_ten_minutes():
    # The boundary tests below derive their inputs *from* the constant, so
    # they hold for any cap value. This is the one place the cap's actual
    # size is pinned -- it is documented in the README and rendered into the
    # 400 body callers see, so a change here is a contract change.
    #
    # Was 1 hour until the graph analysis mode landed: that mode issues one
    # LLM call per non-empty minute, so the window now bounds LLM cost and
    # request duration too.
    assert MAX_TIME_RANGE_DURATION == timedelta(minutes=10)


def test_accepts_range_exactly_at_the_limit():
    # The cap is inclusive. Constructing without raising *is* the assertion:
    # the previous `tr.end - tr.start == MAX_TIME_RANGE_DURATION` was a fact
    # about datetime arithmetic, true for any cap, and could not fail.
    TimeRange(start=FROM, end=FROM + MAX_TIME_RANGE_DURATION)


def test_rejects_range_longer_than_the_limit():
    # Pins the rendered cap in the message, not just "contains a 1" -- the
    # previous match="1" would have accepted almost any rejection message,
    # including one from a different check entirely.
    with pytest.raises(InvalidTimeRangeError, match="최대 0:10:00를 초과"):
        TimeRange(start=FROM, end=FROM + MAX_TIME_RANGE_DURATION + timedelta(seconds=1))


def test_internal_one_minute_segments_are_never_rejected_by_the_cap():
    # TimeRange also represents the one-minute segments the ClickHouse
    # adapter builds internally -- those must never trip the cap.
    # Not raising is the whole check.
    TimeRange(start=FROM, end=FROM + timedelta(minutes=1))


def test_rejects_aware_start_with_naive_end():
    # Comparing/subtracting across awareness raises TypeError, which is not
    # the domain rejection and so escaped as a 500 for what is caller
    # -controlled input. It must be the domain error instead.
    with pytest.raises(InvalidTimeRangeError, match="시간대"):
        TimeRange(start=FROM.replace(tzinfo=timezone.utc), end=TO)


def test_rejects_naive_start_with_aware_end():
    with pytest.raises(InvalidTimeRangeError, match="시간대"):
        TimeRange(start=FROM, end=TO.replace(tzinfo=timezone.utc))


def test_accepts_two_aware_datetimes():
    # The awareness guard must reject only *mixed* pairs; a consistently
    # aware range is valid input.
    TimeRange(start=FROM.replace(tzinfo=timezone.utc), end=TO.replace(tzinfo=timezone.utc))
