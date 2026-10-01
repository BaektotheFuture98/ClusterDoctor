"""Immutable data exchanged by the minute-analysis workflow."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from cluster_doctor.incident_analysis_agent.model.evidence import (
    Evidence,
    EvidenceProvenance,
)


@dataclass(frozen=True)
class RawRecord:
    record_id: int
    event_time: datetime
    line: str
    node_id: str | None = None
    node_name: str | None = None
    severity: str | None = None
    raw: str | None = None
    provenance: EvidenceProvenance | None = None
    raw_kind: Literal["log", "record", "query"] = "log"
    raw_truncated: bool = False
    time_origin: Literal["parsed", "inherited", "fallback"] = "parsed"

    def as_prompt_line(self) -> str:
        return f"#{self.record_id} {self.line}"


@dataclass(frozen=True)
class MinuteBucket:
    minute: datetime
    records: list[RawRecord]


@dataclass(frozen=True)
class SelectedRecord:
    record_id: int
    event_type: str = ""
    reason: str = ""


@dataclass(frozen=True)
class MinuteResult:
    minute: datetime
    selected: list[SelectedRecord] = field(default_factory=list)
    failed: bool = False
    record_count: int = 0


@dataclass(frozen=True)
class AnalysisResult:
    evidence: list[Evidence]
    analyzed_minutes: int = 0
    failed_minutes: int = 0
    reduce_degraded: bool = False
    failed_minutes_at: tuple = ()

    @property
    def fully_failed(self) -> bool:
        return (
            self.analyzed_minutes > 0 and self.failed_minutes == self.analyzed_minutes
        )


def group_into_buckets(records: list[RawRecord]) -> list[MinuteBucket]:
    grouped: dict[datetime, list[RawRecord]] = {}
    for record in records:
        minute = record.event_time.replace(second=0, microsecond=0)
        grouped.setdefault(minute, []).append(record)
    return [MinuteBucket(minute=m, records=grouped[m]) for m in sorted(grouped)]
