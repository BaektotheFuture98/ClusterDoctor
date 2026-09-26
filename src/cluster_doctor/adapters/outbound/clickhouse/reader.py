import logging
from datetime import datetime, timedelta

from cluster_doctor.domain.diagnosis.log_entries import (
    LogEntry,
    NodeLogEntry,
    NodeMetricEntry,
    QueryLogEntry,
    SlowlogEntry,
)
from cluster_doctor.domain.diagnosis.time_range import (
    InvalidTimeRangeError,
    TimeRange,
)
from cluster_doctor.application.ports.log_repository import (
    DEFAULT_NODE_LOG_LIMIT,
    LogRepository,
    clamp_node_log_limit,
)

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
    def __init__(
        self,
        client,
        slowlog_table: str,
        log_table: str,
        node_metric_table: str,
        node_log_table: str,
    ):
        self._client             = client
        self._slowlog_table      = slowlog_table
        self._log_table          = log_table
        self._node_metric_table  = node_metric_table
        self._node_log_table     = node_log_table

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
        the row count shown to the model is derived from what was fetched,
        so a truncated segment tells the model a capped number is the true
        total. Making the count exact would need a second
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

    def fetch_node_logs(
        self,
        start: datetime,
        end: datetime,
        *,
        node: str = "",
        node_role: str = "",
        levels: tuple[str, ...] = (),
        loggers: tuple[str, ...] = (),
        keyword: str = "",
        limit: int = DEFAULT_NODE_LOG_LIMIT,
    ) -> list[NodeLogEntry]:
        """노드 로그를 조건으로 걸러 시간순으로 조회한다.

        조건은 전부 바인딩 파라미터로 넘긴다. 노드 이름·키워드는 LLM이 정하는
        값이므로 SQL에 이어 붙이면 그대로 주입 경로가 된다. 테이블 이름만
        문자열로 들어가는데, 그것은 설정에서 오고 요청 경로에 닿지 않는다.

        ``ORDER BY timestamp``를 붙인 이유는 다른 조회들과 반대다. 세그먼트
        조회는 정렬 없이 자르지만(그쪽은 자름 자체가 결함으로 기록돼 있다),
        노드 로그는 사람이 시간순으로 읽는 것이고 상한에 걸렸을 때 무엇이
        남는지가 정해져 있어야 한다. 사고의 시작을 보는 것이 목적이므로
        **가장 이른 쪽**을 남긴다 — SSH 시절 ``tail``이 남기던 것과 반대다.
        """
        _validate_span(start, end)
        # 음수·과대 요청을 막는다. LLM이 정하는 값이다. 조이는 규칙을 포트에
        # 둔 이유는 호출부가 같은 값을 알아야 절단을 보고할 수 있기 때문이다.
        limit = clamp_node_log_limit(limit)

        clauses = ["timestamp >= %(from_)s", "timestamp < %(to)s"]
        params: dict = {"from_": start, "to": end, "limit": limit}

        if node:
            clauses.append("node = %(node)s")
            params["node"] = node
        if node_role:
            clauses.append("node_role = %(node_role)s")
            params["node_role"] = node_role

        # levels와 loggers는 서로 OR다. 레벨이 높으면 로거와 무관하게 받고,
        # INFO라도 지정한 로거면 받는다. 실측에서 샤드 할당(AllocationService)이
        # INFO였고 같은 INFO의 90%가 진단 무관 잡음이었기 때문이다 — 레벨만
        # 쓰면 앞을 잃고, 레벨을 열면 뒤가 프롬프트를 채운다.
        level_or_logger: list[str] = []

        normalized_levels = _normalize_levels(levels)
        if normalized_levels:
            # level은 ES가 남긴 값, detected_level은 수집기가 추론한 값이다
            # ("WARN" 대 "warn"). 표기가 갈리므로 양쪽을 정규화해 비교하고,
            # 둘 중 하나만 맞아도 통과시킨다. 한쪽만 보면 수집기 설정이 바뀔 때
            # 조용히 0건이 된다.
            level_or_logger.append("upper(trimBoth(level)) IN %(levels)s")
            level_or_logger.append("upper(trimBoth(detected_level)) IN %(levels)s")
            params["levels"] = normalized_levels

        normalized_loggers = _normalize_loggers(loggers)
        if normalized_loggers:
            # 저장된 logger에는 뒤쪽 공백 패딩이 붙어 있다(실측). trimBoth가
            # 없으면 정확 일치가 전부 0건이 된다.
            level_or_logger.append("trimBoth(logger) IN %(loggers)s")
            params["loggers"] = normalized_loggers

        if level_or_logger:
            clauses.append("(" + " OR ".join(level_or_logger) + ")")

        if keyword:
            clauses.append("positionCaseInsensitive(line, %(keyword)s) > 0")
            params["keyword"] = keyword

        sql = (
            "SELECT timestamp, node, node_role, level, detected_level, "
            "logger, filename, host, line "
            f"FROM {self._node_log_table} "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY timestamp LIMIT %(limit)s"
        )
        rows = self._client.query(sql, parameters=params).result_rows

        if len(rows) >= limit:
            _logger.warning(
                "node_log hit the row limit %d for %s ~ %s "
                "(node=%s role=%s levels=%s loggers=%d keyword=%s); "
                "only the earliest rows are returned",
                limit,
                start.isoformat(),
                end.isoformat(),
                node or "-",
                node_role or "-",
                ",".join(normalized_levels) or "-",
                len(normalized_loggers),
                keyword or "-",
            )

        # row 인덱스: 0=timestamp, 1=node, 2=node_role, 3=level,
        #             4=detected_level, 5=logger, 6=filename, 7=host, 8=line
        return [
            NodeLogEntry(
                timestamp=row[0],
                node=row[1],
                node_role=row[2],
                level=row[3],
                detected_level=row[4],
                logger=row[5],
                filename=row[6],
                host=row[7],
                line=row[8],
            )
            for row in rows
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


def _normalize_loggers(loggers: tuple[str, ...]) -> tuple[str, ...]:
    """로거 이름의 공백만 걷어낸다. 대소문자는 건드리지 않는다.

    ES 클래스 이름이므로 대소문자가 의미를 가진다(``o.e.c.c.Coordinator``).
    반면 저장된 값에는 **뒤쪽 공백이 붙어 있다** — ES가 평문 로그에서 로거명을
    패딩하고 수집기가 그대로 실었다(실측: ``"o.e.t.TransportService    "``).
    그래서 조회 쪽은 ``trimBoth(logger)``로 비교하고, 여기서는 넘어온 값의
    공백만 정리한다. 이 두 가지가 없으면 정확 일치가 전부 0건이 된다.
    """
    return tuple(sorted({logger.strip() for logger in loggers if logger.strip()}))


def _normalize_levels(levels: tuple[str, ...]) -> tuple[str, ...]:
    """레벨 표기를 대문자로 맞추고 빈 값을 걷어낸다.

    빈 튜플이 되면 호출부가 IN 절 자체를 빼야 한다. ``IN ()``은 ClickHouse에서
    문법 오류이므로, 사용자가 ``levels=("", " ")``를 준 경우 조건 없는 조회로
    떨어뜨리는 편이 낫다.
    """
    return tuple(sorted({level.strip().upper() for level in levels if level.strip()}))


def _validate_span(start: datetime, end: datetime) -> None:
    """``TimeRange``의 10분 상한 없이 순서·시간대만 검증한다.

    naive와 aware를 섞으면 아래 비교가 TypeError로 터지고, 그 예외는 도메인
    거절이 아니라 내부 오류로 보고된다. ``TimeRange.__post_init__``과 같은
    판정을 같은 예외 타입으로 낸다 — 호출부가 두 경로를 구별할 이유가 없다.
    """
    if start is None or end is None:
        raise InvalidTimeRangeError("start와 end는 None일 수 없습니다")
    if (start.utcoffset() is None) != (end.utcoffset() is None):
        raise InvalidTimeRangeError(
            "start와 end의 시간대 정보가 서로 달라 비교할 수 없습니다 "
            "(한쪽은 timezone-aware, 다른 한쪽은 naive)"
        )
    if not start < end:
        raise InvalidTimeRangeError("start는 end보다 이전이어야 합니다")


def _split_by_minute(time_range: TimeRange) -> list[TimeRange]:
    segments: list[TimeRange] = []
    current = time_range.start
    while current < time_range.end:
        next_minute  = current.replace(second=0, microsecond=0) + timedelta(minutes=1)
        segment_end  = min(next_minute, time_range.end)
        segments.append(TimeRange(start=current, end=segment_end))
        current = segment_end
    return segments
