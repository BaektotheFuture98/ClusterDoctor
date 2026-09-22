from dataclasses import dataclass
from datetime import datetime, timedelta

# Caps query fan-out (the ClickHouse adapter issues one query per source per
# one-minute segment) and memory use (all rows are buffered before sorting).
# Enforced here -- in the domain model -- rather than in a single entry point
# so the limit holds for every caller, present and future. One-minute
# segments built internally by the adapter are always well under this, so
# they are never rejected by it.
#
# The graph analysis mode issues one LLM call per non-empty minute, so this
# window size also bounds LLM cost and how long a single request can run --
# not just ClickHouse fan-out.
MAX_TIME_RANGE_DURATION = timedelta(minutes=10)


class InvalidTimeRangeError(ValueError):
    """Raised when a :class:`TimeRange` is constructed from an invalid pair.

    Subclasses ``ValueError`` so callers doing value-style handling keep
    working, but it is a distinct type so the HTTP layer can map *only* this
    domain rejection to 400. A bare ``ValueError`` handler would also catch
    ``pydantic.ValidationError`` and ``json.JSONDecodeError``, turning
    internal failures into client errors and echoing their messages back.
    """


@dataclass(frozen=True)
class TimeRange:
    start: datetime
    end: datetime

    def __post_init__(self):
        if self.start is None or self.end is None:
            raise InvalidTimeRangeError("start와 end는 None일 수 없습니다")
        # Both the `<` comparison and the subtraction below raise TypeError
        # when one side is timezone-aware and the other naive. TypeError is
        # not the domain rejection, so it escaped the 400 handler and became
        # a generic 500 -- misleading the caller and filling the error log
        # with what is really a bad request. `utcoffset() is None` is the
        # canonical awareness test (a tzinfo whose utcoffset returns None
        # leaves the datetime naive). No input values are echoed.
        if (self.start.utcoffset() is None) != (self.end.utcoffset() is None):
            raise InvalidTimeRangeError(
                "start와 end의 시간대 정보가 서로 달라 비교할 수 없습니다 "
                "(한쪽은 timezone-aware, 다른 한쪽은 naive)"
            )
        # 둘 다 naive인 것도 막는다. 한쪽만 naive인 경우만 막던 동안 이 타입은
        # "서로 비교 가능한 구간"은 보장했지만 "**다른 구간과** 비교 가능한
        # 구간"은 보장하지 못했다. 그 틈으로 모델이 offset 없이 적어 보낸 값이
        # 두 번 들어왔고, 두 번 다 터진 자리는 여기가 아니라 한참 뒤의 차집합
        # 산수였다 — 만들어진 자리에서 거절해야 어디가 잘못인지 알 수 있다.
        if self.start.utcoffset() is None:
            raise InvalidTimeRangeError(
                "start와 end는 시간대 정보를 가져야 합니다 (naive datetime 거부)"
            )
        if not self.start < self.end:
            raise InvalidTimeRangeError("start는 end보다 이전이어야 합니다")
        if self.end - self.start > MAX_TIME_RANGE_DURATION:
            raise InvalidTimeRangeError(
                f"조회 기간은 최대 {MAX_TIME_RANGE_DURATION}를 초과할 수 없습니다"
            )


def merge_spans(
    ranges: "list[TimeRange] | tuple[TimeRange, ...]",
) -> list[tuple[datetime, datetime]]:
    """겹치거나 맞닿은 구간을 합쳐 ``(start, end)`` 쌍으로 돌려준다.

    ``TimeRange``가 아니라 쌍을 돌려주는 이유: 합친 결과는
    ``MAX_TIME_RANGE_DURATION``을 넘길 수 있다. 10분 상한은 **한 번의 조회가
    부담하는 팬아웃 비용**에서 온 것이므로, 커버리지 산수의 중간값에까지 물릴
    이유가 없다. 여기서 ``TimeRange``를 만들면 정상적인 누적 20분이 예외가 된다.
    """
    merged: list[tuple[datetime, datetime]] = []
    for span in sorted(ranges, key=lambda r: (r.start, r.end)):
        if merged and span.start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], span.end))
        else:
            merged.append((span.start, span.end))
    return merged


def subtract_spans(
    target: TimeRange,
    covered: "list[TimeRange] | tuple[TimeRange, ...]",
) -> list[TimeRange]:
    """``target``에서 이미 덮인 구간을 뺀 나머지를 돌려준다.

    중복 분석을 막는 산수의 전부다. 13:50~14:05를 요청받았는데 14:00~14:10이
    이미 분석됐다면 13:50~14:00만 남는다.

    결과 조각은 ``target``보다 짧으므로 ``MAX_TIME_RANGE_DURATION``을 새로
    넘기지 않는다. 길이가 0이 되는 조각은 버린다 — ``TimeRange``가 그것을
    거부하기도 하지만, 그보다 "새로 볼 것이 없다"는 뜻이기 때문이다.
    """
    remaining: list[tuple[datetime, datetime]] = [(target.start, target.end)]
    for block_start, block_end in merge_spans(list(covered)):
        carved: list[tuple[datetime, datetime]] = []
        for start, end in remaining:
            if block_end <= start or block_start >= end:
                carved.append((start, end))
                continue
            if start < block_start:
                carved.append((start, block_start))
            if block_end < end:
                carved.append((block_end, end))
        remaining = carved
        if not remaining:
            break
    return [TimeRange(start=start, end=end) for start, end in remaining if start < end]


def is_covered(
    target: TimeRange,
    covered: "list[TimeRange] | tuple[TimeRange, ...]",
) -> bool:
    """``target``이 이미 분석된 구간에 통째로 들어가는가."""
    return not subtract_spans(target, covered)


def split_span(start: datetime, end: datetime) -> list[TimeRange]:
    """긴 구간을 ``MAX_TIME_RANGE_DURATION`` 이하 조각으로 나눈다.

    상한이 도메인 불변식이라 13:50~14:05 같은 15분 제안은 ``TimeRange``로 만들
    수조차 없다. 그 제안을 거절하는 대신 쪼개는 이유: 상한의 근거는 **한 번의
    조회가 부담하는 팬아웃 비용**이지 "그 시간대를 보면 안 된다"가 아니다.
    쪼개면 비용은 그대로 묶이고 요청한 범위는 전부 본다.

    ``start >= end``면 빈 목록이다. 예외를 올리지 않는 이유: 이 함수가 받는
    값은 모델이 쓴 제안이거나 합집합 계산의 중간값이고, 둘 다 뒤집힌 구간이
    정상적으로 나올 수 있다.
    """
    pieces: list[TimeRange] = []
    cursor = start
    while cursor < end:
        chunk_end = min(cursor + MAX_TIME_RANGE_DURATION, end)
        pieces.append(TimeRange(start=cursor, end=chunk_end))
        cursor = chunk_end
    return pieces
