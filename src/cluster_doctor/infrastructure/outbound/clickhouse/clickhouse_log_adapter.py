import logging
from datetime import timedelta

from cluster_doctor.domain.model.log_entry import (
    LogEntry,
    NodeMetricEntry,
    QueryLogEntry,
    SlowlogEntry,
)
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.application.port.outbound.log_repository import LogRepository

# Each per-segment query already scopes to a single one-minute window for a
# single source, but a pathological spike (a query storm, a metric-reporting
# loop gone wrong) could still return an unbounded number of rows within
# that minute, and results are fully buffered in memory before sorting.
# 10,000 rows/minute/source is generous headroom over normal traffic for any
# of the three sources here (slow-log entries, query-log entries, or
# per-node metric samples are all naturally in the tens-to-low-thousands per
# minute) while still bounding worst-case memory and transfer size per
# query.
_MAX_ROWS_PER_SEGMENT_PER_SOURCE = 10_000

_logger = logging.getLogger(__name__)


class ClickHouseLogAdapter(LogRepository):
    def __init__(self, client, slowlog_table: str, log_table: str, node_metric_table: str):
        self._client             = client
        self._slowlog_table      = slowlog_table
        self._log_table          = log_table
        self._node_metric_table  = node_metric_table

    def fetch_logs(self, time_range: TimeRange) -> list[LogEntry]:
        all_logs: list[LogEntry] = []
        for seg in _split_by_minute(time_range):
            all_logs.extend(self._fetch_slowlogs(seg))
            all_logs.extend(self._fetch_query_logs(seg))
            all_logs.extend(self._fetch_node_metrics(seg))
        all_logs.sort(key=lambda x: x.timestamp, reverse=True)
        return all_logs

    def _query_segment(self, sql: str, tr: TimeRange, source: str) -> list:
        """Run one per-segment, per-source query and flag silent truncation.

        The ``LIMIT`` has no ``ORDER BY`` behind it, so once the cap is hit
        ClickHouse returns an arbitrary subset and the rows that were dropped
        leave no trace in the result. That silence also corrupts the prompt:
        ``_build_prompt`` derives ``총 {len(entries)}건`` from what was
        fetched, so a truncated segment tells the model a capped number is
        the true total. Making the count exact would need a second
        ``count()`` round-trip per segment per source; this at least gives
        operators a signal that it happened, and where.

        The warning text is ASCII on purpose: the root ``StreamHandler``
        writes to ``sys.stderr``, which Python encodes with the OS locale
        (cp949 on a Korean Windows host), so Korean here would reach a
        UTF-8 log aggregator as mojibake. ``>=`` rather than ``==`` because a
        ``LIMIT`` can never be exceeded -- if it somehow is, that is even more
        worth reporting.
        """
        result = self._client.query(sql, parameters={"from_": tr.start, "to": tr.end})
        rows = result.result_rows
        if len(rows) >= _MAX_ROWS_PER_SEGMENT_PER_SOURCE:
            _logger.warning(
                "source=%s hit the per-segment LIMIT %d for segment %s ~ %s; "
                "rows were likely truncated (no ORDER BY, so the kept subset "
                "is arbitrary) and the total reported to the LLM understates "
                "the real count",
                source,
                _MAX_ROWS_PER_SEGMENT_PER_SOURCE,
                tr.start.isoformat(),
                tr.end.isoformat(),
            )
        return rows

    def _fetch_slowlogs(self, tr: TimeRange) -> list[LogEntry]:
        """slowlog를 *발생* 시각 기준으로 조회한다.

        ``ch_ingested_at``이 아니라 ``_source.@timestamp``로 거르는 이유:
        전자는 ClickHouse 적재 시각이고 후자가 ES가 slowlog를 남긴 실제 시각이다.
        실측 지연은 23~41초(평균 31초)로, 분 경계를 넘기는 것만으로 트리거를
        유발한 바로 그 slowlog가 조회 구간에서 빠진다. 같은 1분 창을 두 컬럼으로
        조회하면 실제로 다른 집합이 나온다(실측: 4건 대 2건). 적재 시각으로
        버킷을 나누면 ``split_by_minute``이 붙이는 분 라벨도 통째로 밀려,
        리포트가 지목하는 시각이 사고 시각과 어긋난다.

        ``_source``를 통째로 가져오지 않는 이유는 두 가지다. clickhouse-connect는
        JSON 타입을 ``dict``로 돌려주므로 ``SlowlogEntry``의 문자열 필드에 dict가
        들어가고, 행당 2.7KB 중 진단에 쓰이는 것은 27%뿐이다 -- 나머지
        (``host.mac``, ``agent.ephemeral_id``, ``host.os.kernel`` 등)는
        프롬프트 토큰만 먹는다. 필요한 서브컬럼만 이름으로 투영한다.
        """
        sql    = (
            "SELECT _source.`@timestamp`, "
            "_source.elasticsearch.index.name, _source.elasticsearch.node.name, "
            "_source.elasticsearch.slowlog.took, _source.elasticsearch.slowlog.total_hits, "
            "_source.elasticsearch.slowlog.total_shards, _source.elasticsearch.slowlog.id, "
            f"_source.elasticsearch.slowlog.source FROM {self._slowlog_table} "
            "WHERE _source.`@timestamp` >= %(from_)s AND _source.`@timestamp` < %(to)s "
            f"LIMIT {_MAX_ROWS_PER_SEGMENT_PER_SOURCE}"
        )
        # row 인덱스: 0=발생 시각, 1=인덱스명, 2=노드명, 3=took,
        #             4=total_hits, 5=total_shards, 6=x-opaque-id, 7=쿼리 원문
        return [
            SlowlogEntry(
                timestamp=row[0],
                index_name=row[1],
                node=row[2],
                took=row[3],
                total_hits=row[4],
                total_shards=row[5],
                opaque_id=row[6],
                query=row[7],
            )
            for row in self._query_segment(sql, tr, "slowlog")
        ]

    def _fetch_query_logs(self, tr: TimeRange) -> list[LogEntry]:
        sql    = (
            f"SELECT reg_date, host, run_time, success, cmd, service, env, project, cluster, keyword, company, user "
            f"FROM {self._log_table} "
            "WHERE reg_date >= %(from_)s AND reg_date < %(to)s "
            f"LIMIT {_MAX_ROWS_PER_SEGMENT_PER_SOURCE}"
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
            for row in self._query_segment(sql, tr, "es_query_log")
        ]

    def _fetch_node_metrics(self, tr: TimeRange) -> list[LogEntry]:
        sql    = (
            f"SELECT reg_date, node_name, node_ip, os_cpu_percent, os_mem_used_percent, "
            "process_cpu_percent, jvm_heap_used_percent, "
            "search_active, search_queue, search_rejected, "
            f"write_active, write_queue, write_rejected FROM {self._node_metric_table} "
            "WHERE reg_date >= %(from_)s AND reg_date < %(to)s "
            f"LIMIT {_MAX_ROWS_PER_SEGMENT_PER_SOURCE}"
        )
        return [
            NodeMetricEntry(
                timestamp=row[0],
                node_name=row[1],
                node_ip=row[2],
                os_cpu_percent=row[3],
                os_mem_used_percent=row[4],
                process_cpu_percent=row[5],
                jvm_heap_used_percent=row[6],
                search_active=row[7],
                search_queue=row[8],
                search_rejected=row[9],
                write_active=row[10],
                write_queue=row[11],
                write_rejected=row[12],
            )
            for row in self._query_segment(sql, tr, "node_metric")
        ]


def _split_by_minute(time_range: TimeRange) -> list[TimeRange]:
    segments: list[TimeRange] = []
    current = time_range.start
    while current < time_range.end:
        next_minute  = current.replace(second=0, microsecond=0) + timedelta(minutes=1)
        segment_end  = min(next_minute, time_range.end)
        segments.append(TimeRange(start=current, end=segment_end))
        current = segment_end
    return segments
