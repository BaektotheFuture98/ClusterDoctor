from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class TimeRange:
    start: datetime
    end: datetime

    def __post_init__(self):
        if self.start is None or self.end is None:
            raise ValueError("start와 end는 None일 수 없습니다")
        if not self.start < self.end:
            raise ValueError("start는 end보다 이전이어야 합니다")
