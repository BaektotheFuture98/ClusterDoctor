from dataclasses import dataclass
from decimal import Decimal
from typing import ClassVar

from cluster_doctor.domain.model.log_entry import LogEntry


@dataclass(frozen=True)
class QueryLogEntry(LogEntry):
    """ES 쿼리 실행 기록 한 건. company·user가 사는 곳이다."""

    source: ClassVar[str] = "es_query_log"

    host: str
    run_time: Decimal
    success: bool
    cmd: str
    service: str
    env: str
    project: str
    cluster: str
    # tuple이다. list를 필드로 두면 frozen이어도 해시가 깨져 중복 제거·집합
    # 연산에 쓸 수 없다.
    keywords: tuple[str, ...]
    company: str | None
    user: str | None
