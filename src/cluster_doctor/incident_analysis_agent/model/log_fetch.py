"""병렬 로그 조회 결과와 소스별 실패 정보."""

from dataclasses import dataclass

from cluster_doctor.incident_analysis_agent.model.log_entries import LogEntry
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange


@dataclass(frozen=True)
class LogSourceFailure:
    source: str
    window: TimeRange
    error: str


@dataclass(frozen=True)
class LogFetchResult:
    entries: tuple[LogEntry, ...] = ()
    failures: tuple[LogSourceFailure, ...] = ()
