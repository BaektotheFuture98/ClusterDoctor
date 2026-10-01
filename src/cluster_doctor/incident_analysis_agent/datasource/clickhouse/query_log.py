"""es_query_log datasource: ClickHouse 조회 + 선별 설정 + 레코드 변환.

slowlog와 달리 **성공한 요청도 들어온다.** 그래서 이쪽에서는 실패와 급증이
정보이고, 정상 응답 시간의 요청은 배경이다.
"""

from __future__ import annotations

from dataclasses import fields
from datetime import UTC, datetime

from cluster_doctor.incident_analysis_agent.datasource.clickhouse.client import (
    MAX_ROWS_PER_SEGMENT_PER_SOURCE,
    query_segment,
)
from cluster_doctor.incident_analysis_agent.model.evidence import (
    EvidenceProvenance,
    EvidenceSource,
)
from cluster_doctor.incident_analysis_agent.model.log_entries import (
    LogEntry,
    QueryLogEntry,
    record_json,
)
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.model import (
    RawRecord,
)
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.spec import (
    AnalysisSpec,
)

SPEC = AnalysisSpec(
    source=EvidenceSource.QUERY_LOG,
    label="es_query_log (쿼리 실행 기록)",
    what_matters=(
        "- success=False인 요청. 사용자가 실제로 오류를 받은 것이다.\n"
        "- run_time이 같은 구간의 다른 요청보다 뚜렷하게 큰 것.\n"
        "- 특정 company/user/project에 몰린 요청.\n"
        "- 평소에 없던 형태의 cmd."
    ),
    what_is_noise=(
        "- success=True이고 run_time이 평범한 요청.\n"
        "- 같은 cmd가 주기적으로 반복되는 것 (대표 한 줄만 남긴다).\n"
        "- 헬스체크·모니터링 성격의 짧은 조회."
    ),
)


def fetch(client, table: str, tr: TimeRange) -> list[LogEntry]:
    sql = (
        f"SELECT * FROM {table} "
        "WHERE reg_date >= %(from_)s AND reg_date < %(to)s "
        f"LIMIT {MAX_ROWS_PER_SEGMENT_PER_SOURCE}"
    )
    rows = query_segment(client, sql, tr, "es_query_log", named=True)
    provenance = EvidenceProvenance(
        method="clickhouse",
        collected_at=datetime.now(UTC),
        table=table,
        query_from=tr.start,
        query_to=tr.end,
        excerpt=len(rows) >= MAX_ROWS_PER_SEGMENT_PER_SOURCE,
    )
    known = {f.name for f in fields(QueryLogEntry)} - {
        "provenance",
        "additional_fields",
    }
    entries = []
    for row in rows:
        values = {name: row[name] for name in known}
        values["keyword"] = tuple(values["keyword"])
        entries.append(
            QueryLogEntry(
                **values,
                provenance=provenance,
                additional_fields={
                    name: value for name, value in row.items() if name not in known
                },
            )
        )
    return entries


def to_records(entries: list[QueryLogEntry]) -> list[RawRecord]:
    ordered = sorted(entries, key=lambda entry: entry.timestamp)
    return [
        RawRecord(
            record_id=index,
            event_time=entry.timestamp,
            raw=record_json(entry),
            raw_kind="record",
            provenance=entry.provenance,
            line=(
                f"run_time={entry.run_time} success={entry.success} "
                f"host={entry.host or '?'} service={entry.service or '?'} "
                f"company={entry.company or '?'} user={entry.user or '?'} "
                f"cmd={entry.cmd or '(없음)'} keyword={list(entry.keyword)} "
                f"s_date={entry.s_date} e_date={entry.e_date} date_range={entry.date_range} "
                f"keyword_count={entry.keyword_count} search_count={entry.search_count}"
            ),
            node_name=entry.host or None,
            severity="ERROR" if entry.is_success is False else None,
        )
        for index, entry in enumerate(ordered, start=1)
    ]
