"""ClickHouse 공통 client와 조회 오케스트레이션.

목적별 쿼리와 row 매핑은 같은 디렉터리의 ``slowlog.py``/``query_log.py``/
``node_metric.py``/``master_log.py``에 있다. 여기 남는 것은 connection 하나를
여럿이 공유하기 위한 공통 코드와, 그 목적별 함수들을 호출하는 오케스트레이션이다.
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.basemodel.log_entries import (
    LogEntry,
    NodeLogEntry,
)
from cluster_doctor.incident_analysis_agent.model.basemodel.time_range import (
    InvalidTimeRangeError,
    TimeRange,
)

# 노드 로그 한 번 조회로 돌려줄 기본/최대 줄 수. 프롬프트에 그대로 실리므로
# 상한을 둔다 — WARN 폭주 구간은 1분에 수천 줄이 쌓인다.
DEFAULT_NODE_LOG_LIMIT = 300
MAX_NODE_LOG_LIMIT = 2000

# Each per-segment query already scopes to a single one-minute window for a
# single source, but a pathological spike (a query storm, a metric-reporting
# loop gone wrong) could still return an unbounded number of rows within
# that minute, and results are fully buffered in memory before sorting.
# 10,000 rows/minute/source is generous headroom over normal traffic for any
# of the three sources here (slow-log entries, query-log entries, or
# per-node metric samples are all naturally in the tens-to-low-thousands per
# minute) while still bounding worst-case memory and transfer size per
# query.
MAX_ROWS_PER_SEGMENT_PER_SOURCE = 10_000

_logger = logging.getLogger(__name__)


def clamp_node_log_limit(limit: int) -> int:
    """LLM이 준 줄 수 요청을 실제 적용값으로 바꾼다.

    호출부와 구현부가 **같은 값을 알아야** 한다. 어댑터만 조여 놓으면 tool은
    "몇 줄이 잘렸는지"를 모른 채 원래 요청값과 비교하게 되고,
    ``max_lines=5000``을 받아 2000건이 돌아왔을 때 ``2000 >= 5000``이 거짓이라
    절단 사실을 알리지 못한다 — 모델은 창 전체를 본 줄로 알고 추론한다.
    반대로 ``0``은 1로 조여지는데 그것을 모르면 "상한 0줄에 걸려"라고 쓴다.
    """
    return max(1, min(int(limit), MAX_NODE_LOG_LIMIT))


class LogRepository(ABC):
    """진단에 필요한 로그와 메트릭을 조회하는 외부 저장소 포트.

    어댑터가 소스 데이터를 LogEntry 계열로 돌려주며 근거 선별은 호출자가 맡는다.
    """

    @abstractmethod
    def fetch_logs(self, time_range: TimeRange) -> list[LogEntry]:
        """분 단위 분석에 쓰는 세 소스(slowlog·쿼리 로그·노드 메트릭)를 한 리스트로."""
        ...

    @abstractmethod
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
        """ES 노드 로그를 조건으로 걸러 시간순으로 돌려준다.

        ``TimeRange``를 받지 않는 것은 의도적이다. 그 타입은 10분 상한을
        강제하는데, 그 상한은 "소스당 1분마다 쿼리 하나 + 분마다 LLM 호출
        하나"라는 분석 파이프라인의 팬아웃 비용에서 온 것이다. 이 조회는
        단일 쿼리이고 비용이 ``limit``으로 이미 묶여 있어 같은 상한을 물려받을
        이유가 없다 — 사고 전체 구간을 한 번에 보는 것이 이 조회의 용도다.

        빈 문자열·빈 튜플은 "그 조건으로 걸러내지 않는다"는 뜻이다.

        ``levels``와 ``loggers``만 서로 **OR**로 묶이고, 나머지 조건은 AND다.
        레벨이 높으면 로거와 무관하게 받고, INFO라도 지정한 로거면 받는다는
        뜻이다.

        Args:
            start:     조회 시작(포함). timezone-aware여야 한다.
            end:       조회 종료(제외). timezone-aware여야 한다.
            node:      노드 이름 정확히 일치.
            node_role: 노드 역할 정확히 일치. 마스터 로그는 여기에 "master".
            levels:    로그 레벨. ``("WARN", "ERROR")`` 형태. 대소문자 무시.
            loggers:   ES 로거 이름. 축약형이다. ``("o.e.c.c.Coordinator",)``.
                       대소문자를 구분한다 — 클래스 이름이기 때문이다.
            keyword:   ``line`` 부분 일치. 대소문자 무시.
            limit:     최대 줄 수. ``MAX_NODE_LOG_LIMIT``으로 잘린다.
        """
        ...


def query_segment(client, sql: str, tr: TimeRange, source: str) -> list:
    """한 세그먼트·소스 조회를 실행하고 무음 절단을 로그로 알린다.

    ``LIMIT``에 ``ORDER BY``가 없어 상한에 걸리면 ClickHouse가 임의의 부분집합을
    돌려주고, 빠진 행은 결과에 흔적을 남기지 않는다.
    """
    result = client.query(sql, parameters={"from_": tr.start, "to": tr.end})
    rows = result.result_rows
    if len(rows) >= MAX_ROWS_PER_SEGMENT_PER_SOURCE:
        _logger.warning(
            "source=%s hit the per-segment LIMIT %d for segment %s ~ %s; "
            "rows were likely truncated (no ORDER BY, so the kept subset "
            "is arbitrary) and the total reported to the LLM understates "
            "the real count",
            source,
            MAX_ROWS_PER_SEGMENT_PER_SOURCE,
            tr.start.isoformat(),
            tr.end.isoformat(),
        )
    return rows


def normalize_loggers(loggers: tuple[str, ...]) -> tuple[str, ...]:
    """로거 이름의 공백만 걷어낸다. 대소문자는 건드리지 않는다.

    ES 클래스 이름이므로 대소문자가 의미를 가진다(``o.e.c.c.Coordinator``).
    반면 저장된 값에는 **뒤쪽 공백이 붙어 있다** — ES가 평문 로그에서 로거명을
    패딩하고 수집기가 그대로 실었다(실측: ``"o.e.t.TransportService    "``).
    그래서 조회 쪽은 ``trimBoth(logger)``로 비교하고, 여기서는 넘어온 값의
    공백만 정리한다.
    """
    return tuple(sorted({logger.strip() for logger in loggers if logger.strip()}))


def normalize_levels(levels: tuple[str, ...]) -> tuple[str, ...]:
    """레벨 표기를 대문자로 맞추고 빈 값을 걷어낸다.

    빈 튜플이 되면 호출부가 IN 절 자체를 빼야 한다. ``IN ()``은 ClickHouse에서
    문법 오류이므로, 사용자가 ``levels=("", " ")``를 준 경우 조건 없는 조회로
    떨어뜨리는 편이 낫다.
    """
    return tuple(sorted({level.strip().upper() for level in levels if level.strip()}))


def validate_span(start: datetime, end: datetime) -> None:
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


def split_by_minute(time_range: TimeRange) -> list[TimeRange]:
    segments: list[TimeRange] = []
    current = time_range.start
    while current < time_range.end:
        next_minute = current.replace(second=0, microsecond=0) + timedelta(minutes=1)
        segment_end = min(next_minute, time_range.end)
        segments.append(TimeRange(start=current, end=segment_end))
        current = segment_end
    return segments


class ClickHouseLogAdapter(LogRepository):
    def __init__(
        self,
        client,
        slowlog_table: str,
        log_table: str,
        node_metric_table: str,
        node_log_table: str,
    ):
        self._client = client
        self._slowlog_table = slowlog_table
        self._log_table = log_table
        self._node_metric_table = node_metric_table
        self._node_log_table = node_log_table

    def fetch_logs(self, time_range: TimeRange) -> list[LogEntry]:
        from cluster_doctor.incident_analysis_agent.datasource.clickhouse import (
            node_metric,
            query_log,
            slowlog,
        )

        all_logs: list[LogEntry] = []
        for seg in split_by_minute(time_range):
            all_logs.extend(slowlog.fetch(self._client, self._slowlog_table, seg))
            all_logs.extend(query_log.fetch(self._client, self._log_table, seg))
            all_logs.extend(node_metric.fetch(self._client, self._node_metric_table, seg))
        all_logs.sort(key=lambda x: x.timestamp, reverse=True)
        return all_logs

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
        조회는 정렬 없이 자르지만, 노드 로그는 사람이 시간순으로 읽는 것이고
        상한에 걸렸을 때 **가장 이른 쪽**을 남긴다 — SSH ``tail``과 반대다.
        """
        validate_span(start, end)
        limit = clamp_node_log_limit(limit)

        clauses = ["timestamp >= %(from_)s", "timestamp < %(to)s"]
        params: dict = {"from_": start, "to": end, "limit": limit}

        if node:
            clauses.append("node = %(node)s")
            params["node"] = node
        if node_role:
            clauses.append("node_role = %(node_role)s")
            params["node_role"] = node_role

        level_or_logger: list[str] = []

        normalized_levels = normalize_levels(levels)
        if normalized_levels:
            level_or_logger.append("upper(trimBoth(level)) IN %(levels)s")
            level_or_logger.append("upper(trimBoth(detected_level)) IN %(levels)s")
            params["levels"] = normalized_levels

        normalized_loggers = normalize_loggers(loggers)
        if normalized_loggers:
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
