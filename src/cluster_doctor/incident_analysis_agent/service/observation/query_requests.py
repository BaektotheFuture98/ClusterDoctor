"""분석과 보고서 전달이 공유하는 개별 execution 뷰."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING

from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry, query_record_key
if TYPE_CHECKING:
    from cluster_doctor.incident_analysis_agent.model.observations import SlowCandidate


def valid_runtime(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return number if number.is_finite() and number >= 0 else None


@dataclass(frozen=True)
class QueryRequestView:
    record: QueryLogEntry
    record_key: str
    ordinal: int
    execution_seconds: Decimal | None
    target_host: str | None
    index_name: str | None
    conditions: tuple[str, ...]


def _view(record: QueryLogEntry, ordinal: int) -> QueryRequestView:
    return QueryRequestView(record, query_record_key(record), ordinal,
        valid_runtime(record.run_time), record.target_host, record.index_name, record.conditions)


def rank_query_requests(requests: tuple[QueryLogEntry, ...]) -> tuple[QueryRequestView, ...]:
    rows = [_view(record, i) for i, record in enumerate(requests)]
    return tuple(sorted(rows, key=lambda r: (
        r.execution_seconds is None,
        -r.execution_seconds if r.execution_seconds is not None else Decimal(0),
        r.record.timestamp, r.ordinal)))


def matching_candidate(record: QueryLogEntry, requests: tuple[QueryLogEntry, ...],
                       candidates: tuple[SlowCandidate, ...]) -> SlowCandidate | None:
    key = query_record_key(record)
    if sum(query_record_key(r) == key for r in requests) != 1:
        return None
    matches = [c for c in candidates if c.source == QueryLogEntry.source and c.query_record_key == key]
    return matches[0] if len(matches) == 1 else None
