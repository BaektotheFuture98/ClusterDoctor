import pytest
from datetime import datetime, timedelta, timezone
from cluster_doctor.contracts.time_range import (
    MAX_TIME_RANGE_DURATION,
    InvalidTimeRangeError,
    TimeRange,
)

# TimeRange는 naive datetime을 거부한다. 기본 재료를 aware로 두고, 거절을
# 확인하는 테스트만 naive를 따로 만든다 — 반대로 두면 대부분의 테스트가
# "naive 거부"에 걸려 무엇을 검증하려던 것이었는지 알 수 없게 된다.
FROM = datetime(2026, 8, 20, 2, 9, 0, tzinfo=timezone.utc)
TO   = datetime(2026, 8, 20, 2, 10, 0, tzinfo=timezone.utc)

NAIVE_FROM = FROM.replace(tzinfo=None)
NAIVE_TO   = TO.replace(tzinfo=None)

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
        TimeRange(start=FROM, end=NAIVE_TO)


def test_rejects_naive_start_with_aware_end():
    with pytest.raises(InvalidTimeRangeError, match="시간대"):
        TimeRange(start=NAIVE_FROM, end=TO)


def test_rejects_both_naive_datetimes():
    """둘 다 naive인 것도 막는다.

    한쪽만 naive인 경우만 막던 동안 이 타입은 "서로 비교 가능한 구간"은
    보장했지만 "**다른 구간과** 비교 가능한 구간"은 보장하지 못했다. 그 틈으로
    offset 없이 적힌 값이 들어왔고, 터진 자리는 여기가 아니라 한참 뒤의 차집합
    산수였다.
    """
    with pytest.raises(InvalidTimeRangeError, match="naive") as excinfo:
        TimeRange(start=NAIVE_FROM, end=NAIVE_TO)

    # 혼합 쌍과 **다른** 규칙이다. 두 메시지가 같아지면 어느 규칙이 걸렸는지
    # 구분할 수 없고, 고칠 자리도 특정되지 않는다.
    assert "서로 달라" not in str(excinfo.value)


def test_accepts_two_aware_datetimes():
    # A consistently aware range is the only valid input.
    TimeRange(start=FROM, end=TO)
