import logging
from datetime import timedelta

from cluster_doctor.domain.model.log_entry import LogEntry
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
        sql    = (
            f"SELECT ch_ingested_at, _source FROM {self._slowlog_table} "
            "WHERE ch_ingested_at >= %(from_)s AND ch_ingested_at < %(to)s "
            f"LIMIT {_MAX_ROWS_PER_SEGMENT_PER_SOURCE}"
        )
        return [
            LogEntry(timestamp=row[0], level="SLOWLOG", source="slowlog", message=row[1])
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
            LogEntry(
                timestamp=row[0],
                level="SUCCESS" if row[3] == "Y" else "FAIL",
                source="es_query_log",
                component=row[5],
                node=row[1],
                message=(
                    f"[{row[4]}] project={row[7]} env={row[6]} cluster={row[8]} "
                    f"runtime={row[2]}s keyword={row[9]}"
                ),
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
            LogEntry(
                timestamp=row[0],
                level="METRIC",
                source="node_metric",
                node=f"{row[1]} ({row[2]})",
                message=(
                    f"cpu={row[3]}% mem={row[4]}% proc_cpu={row[5]}% jvm_heap={row[6]}% "
                    f"search(active={row[7]},queue={row[8]},rejected={row[9]}) "
                    f"write(active={row[10]},queue={row[11]},rejected={row[12]})"
                ),
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
