from dataclasses import dataclass
from datetime import datetime, timedelta

# Caps query fan-out (the ClickHouse adapter issues one query per source per
# one-minute segment) and memory use (all rows are buffered before sorting).
# Enforced here -- in the domain model -- rather than in a single entry point
# so the limit holds for every caller, present and future. One-minute
# segments built internally by the adapter are always well under this, so
# they are never rejected by it.
MAX_TIME_RANGE_DURATION = timedelta(hours=1)


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
        if not self.start < self.end:
            raise InvalidTimeRangeError("start는 end보다 이전이어야 합니다")
        if self.end - self.start > MAX_TIME_RANGE_DURATION:
            raise InvalidTimeRangeError(
                f"조회 기간은 최대 {MAX_TIME_RANGE_DURATION}를 초과할 수 없습니다"
            )
