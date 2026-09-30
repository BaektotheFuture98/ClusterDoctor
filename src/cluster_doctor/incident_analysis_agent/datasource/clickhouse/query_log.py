"""es_query_log datasource: ClickHouse 조회 + 선별 설정 + 레코드 변환.

slowlog와 달리 **성공한 요청도 들어온다.** 그래서 이쪽에서는 실패와 급증이
정보이고, 정상 응답 시간의 요청은 배경이다.
"""

from __future__ import annotations

from cluster_doctor.incident_analysis_agent.datasource.clickhouse.client import (
    MAX_ROWS_PER_SEGMENT_PER_SOURCE,
    query_segment,
)
from cluster_doctor.incident_analysis_agent.model.evidence import EvidenceSource
from cluster_doctor.incident_analysis_agent.model.log_entries import LogEntry, QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.spec import AnalysisSpec
from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.model import RawRecord

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
        f"SELECT reg_date, host, run_time, success, cmd, service, env, project, cluster, keyword, company, user "
        f"FROM {table} "
        "WHERE reg_date >= %(from_)s AND reg_date < %(to)s "
        f"LIMIT {MAX_ROWS_PER_SEGMENT_PER_SOURCE}"
    )
    # row 인덱스: 0=reg_date, 1=host, 2=run_time, 3=success, 4=cmd,
    #             5=service, 6=env, 7=project, 8=cluster,
    #             9=keyword, 10=company, 11=user
    return [
        QueryLogEntry(
            timestamp=row[0],
            host=row[1],
            run_time=row[2],
            # ClickHouse는 'Y'/'N'을 준다. 도메인까지 그 표현을 끌고 가지 않는다.
            success=row[3] == "Y",
            cmd=row[4],
            service=row[5],
            env=row[6],
            project=row[7],
            cluster=row[8],
            keywords=tuple(row[9] or ()),
            company=row[10] or None,
            user=row[11] or None,
        )
        for row in query_segment(client, sql, tr, "es_query_log")
    ]


def to_records(entries: list[QueryLogEntry]) -> list[RawRecord]:
    ordered = sorted(entries, key=lambda entry: entry.timestamp)
    return [
        RawRecord(
            record_id=index,
            event_time=entry.timestamp,
            line=(
                f"run_time={entry.run_time} success={entry.success} "
                f"host={entry.host or '?'} service={entry.service or '?'} "
                f"company={entry.company or '?'} user={entry.user or '?'} "
                f"cmd={entry.cmd or '(없음)'}"
            ),
            node_name=entry.host or None,
            severity=None if entry.success else "ERROR",
        )
        for index, entry in enumerate(ordered, start=1)
    ]
