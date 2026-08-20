from datetime import timedelta

from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange
from cluster_doctor.domain.port.outbound.log_repository import LogRepository


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

    def _fetch_slowlogs(self, tr: TimeRange) -> list[LogEntry]:
        sql    = (
            f"SELECT ch_ingested_at, _source FROM {self._slowlog_table} "
            "WHERE ch_ingested_at >= %(from_)s AND ch_ingested_at < %(to)s"
        )
        result = self._client.query(sql, parameters={"from_": tr.start, "to": tr.end})
        return [
            LogEntry(timestamp=row[0], level="SLOWLOG", source="slowlog", message=row[1])
            for row in result.result_rows
        ]

    def _fetch_query_logs(self, tr: TimeRange) -> list[LogEntry]:
        sql    = (
            f"SELECT reg_date, host, run_time, success, cmd, service, env, project, cluster, hit_count, keyword "
            f"FROM {self._log_table} "
            "WHERE reg_date >= %(from_)s AND reg_date < %(to)s"
        )
        result = self._client.query(sql, parameters={"from_": tr.start, "to": tr.end})
        return [
            LogEntry(
                timestamp=row[0],
                level="SUCCESS" if row[3] == "Y" else "FAIL",
                source="es_query_log",
                component=row[5],
                node=row[1],
                message=(
                    f"[{row[4]}] project={row[7]} env={row[6]} cluster={row[8]} "
                    f"runtime={row[2]}s keyword={row[10]}"
                ),
            )
            for row in result.result_rows
        ]

    def _fetch_node_metrics(self, tr: TimeRange) -> list[LogEntry]:
        sql    = (
            f"SELECT reg_date, node_name, node_ip, os_cpu_percent, os_mem_used_percent, "
            "process_cpu_percent, jvm_heap_used_percent, "
            "search_active, search_queue, search_rejected, "
            f"write_active, write_queue, write_rejected FROM {self._node_metric_table} "
            "WHERE reg_date >= %(from_)s AND reg_date < %(to)s"
        )
        result = self._client.query(sql, parameters={"from_": tr.start, "to": tr.end})
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
            for row in result.result_rows
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
