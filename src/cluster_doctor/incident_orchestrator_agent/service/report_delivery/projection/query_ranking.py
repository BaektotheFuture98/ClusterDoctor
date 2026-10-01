"""Deterministic request rankings from ClickHouse query-log records, not LLM picks."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.evidence_citation import (
    kst_stamp,
)


def query_type(cmd: str) -> str:
    normalized = cmd.strip().lower()
    return normalized if normalized in ("agg", "search") else "기타"


def duration(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    value = Decimal(str(value))
    return value if value.is_finite() and value >= 0 else None


@dataclass(frozen=True)
class QueryRanking:
    keywords: tuple[str, ...]
    company: str | None
    user: str | None
    cmd: str
    s_date: int
    e_date: int
    date_range: int
    requests: tuple[QueryLogEntry, ...]

    @property
    def query_type(self) -> str:
        return query_type(self.cmd)

    @property
    def count(self) -> int:
        return len(self.requests)

    @property
    def valid_count(self) -> int:
        return sum(duration(r.run_time) is not None for r in self.requests)

    @property
    def total(self) -> Decimal | None:
        values = [duration(r.run_time) for r in self.requests]
        valid = [v for v in values if v is not None]
        return sum(valid, Decimal(0)) if valid else None

    @property
    def average(self) -> Decimal | None:
        return self.total / self.valid_count if self.valid_count else None

    @property
    def peak(self) -> Decimal | None:
        valid = [v for r in self.requests if (v := duration(r.run_time)) is not None]
        return max(valid) if valid else None

    @property
    def first(self) -> datetime:
        return min(r.timestamp for r in self.requests)

    @property
    def last(self) -> datetime:
        return max(r.timestamp for r in self.requests)


def query_ranking(requests: tuple[QueryLogEntry, ...]) -> tuple[QueryRanking, ...]:
    groups = {}
    for r in requests:
        key = (r.keyword, r.company, r.user, r.cmd, r.s_date, r.e_date, r.date_range)
        groups.setdefault(key, []).append(r)
    rankings = [QueryRanking(*key, tuple(rows)) for key, rows in groups.items()]
    return tuple(
        sorted(
            rankings,
            key=lambda r: (
                -(r.average if r.average is not None else Decimal(-1)),
                -(r.peak if r.peak is not None else Decimal(-1)),
                -r.count,
                r.keywords,
                r.company or "",
                r.user or "",
                r.cmd,
            ),
        )
    )


def search_date(value: int) -> str:
    try:
        return date.fromisoformat(str(value)).isoformat()
    except ValueError:
        return f"미확인 (원본 {value})"


def seconds(value: Decimal | None) -> str:
    return f"{value:.3f}초" if value is not None else "미확인"


def ranking_lines(requests: tuple[QueryLogEntry, ...]) -> list[str]:
    if not requests:
        return []
    lines = [
        "ClickHouse 수집 기록 기준 · 키워드 조합/회사/사용자/cmd별 평균 실행 시간 내림차순",
        "검색 시각=reg_date (KST) · 검색 대상 기간=s_date~e_date · 검색 일수=date_range",
    ]
    if any(r.provenance and r.provenance.excerpt for r in requests):
        lines.append("부분 집계: 조회 건수 상한에 도달한 구간이 있습니다.")
    for i, r in enumerate(query_ranking(requests), 1):
        lines.append(
            f"[{i}] keyword={list(r.keywords)} company={r.company or '미확인'} "
            f"user={r.user or '미확인'} cmd={r.cmd or '미확인'} 유형={r.query_type} "
            f"검색 대상={search_date(r.s_date)}~{search_date(r.e_date)} date_range={r.date_range}일 "
            f"{r.count}건 평균={seconds(r.average)} 최대={seconds(r.peak)} 합계={seconds(r.total)} "
            f"유효 실행 시간={r.valid_count}건 첫 검색={kst_stamp(r.first)} 마지막 검색={kst_stamp(r.last)}"
        )
    return lines
