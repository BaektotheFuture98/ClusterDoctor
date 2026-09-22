"""ClickHouseLogAdapter.fetch_node_logs 검증.

노드 이름·키워드·레벨은 LLM이 정하는 값이다. 그래서 여기서 가장 중요한
성질은 조건이 전부 바인딩 파라미터로 나가는 것이고, 그다음이 조건을 비웠을
때 WHERE 절에서 아예 빠지는 것이다(빈 IN 절은 ClickHouse 문법 오류다).
"""

from datetime import datetime, timedelta, timezone

import pytest

from cluster_doctor.application.port.outbound.log_repository import (
    MAX_NODE_LOG_LIMIT,
)
from cluster_doctor.domain.model.clickhouse.node_log_entry import NodeLogEntry
from cluster_doctor.contracts.time_range import InvalidTimeRangeError
from cluster_doctor.infrastructure.outbound.clickhouse.clickhouse_log_adapter import (
    ClickHouseLogAdapter,
)

_KST = timezone(timedelta(hours=9))
_START = datetime(2026, 9, 10, 2, 0, tzinfo=_KST)
_END = datetime(2026, 9, 10, 2, 15, tzinfo=_KST)

_ROW = [
    datetime(2026, 9, 10, 2, 4, 33, tzinfo=_KST),
    "es-data-02",
    "data",
    "WARN ",
    "warn",
    "o.e.i.b.HierarchyCircuitBreakerService",
    "es-prod.log",
    "es-node-02.internal",
    "[gc][young][12345] duration [1.2s], collections [1]/[1.9s]",
]


class _FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class _FakeClient:
    """query 호출을 기록하는 대역. 실제 ClickHouse는 쓰지 않는다."""

    def __init__(self, rows=None):
        self.rows = rows if rows is not None else [_ROW]
        self.sql = None
        self.parameters = None

    def query(self, sql, parameters=None):
        self.sql = sql
        self.parameters = parameters
        return _FakeResult(self.rows)


def _adapter(client) -> ClickHouseLogAdapter:
    return ClickHouseLogAdapter(
        client=client,
        slowlog_table="slowlog_v2",
        log_table="log",
        node_metric_table="es_node_metric",
        node_log_table="es_node_log",
    )


class TestProjection:
    def test_행을_NodeLogEntry로_매핑한다(self):
        client = _FakeClient()

        entries = _adapter(client).fetch_node_logs(_START, _END)

        assert len(entries) == 1
        entry = entries[0]
        assert isinstance(entry, NodeLogEntry)
        assert entry.node == "es-data-02"
        assert entry.node_role == "data"
        assert entry.level == "WARN "
        assert entry.detected_level == "warn"
        assert entry.logger.endswith("HierarchyCircuitBreakerService")
        assert entry.filename == "es-prod.log"
        assert entry.host == "es-node-02.internal"
        assert "duration [1.2s]" in entry.line
        assert entry.source == "node_log"

    def test_설정된_테이블을_조회한다(self):
        client = _FakeClient()

        _adapter(client).fetch_node_logs(_START, _END)

        assert "FROM es_node_log" in client.sql

    def test_시간순으로_정렬한다(self):
        """상한에 걸렸을 때 무엇이 남는지가 정해져 있어야 한다."""
        client = _FakeClient()

        _adapter(client).fetch_node_logs(_START, _END)

        assert "ORDER BY timestamp" in client.sql


class TestFilters:
    def test_조건을_비우면_WHERE에서_빠진다(self):
        client = _FakeClient()

        _adapter(client).fetch_node_logs(_START, _END, levels=())

        assert "node =" not in client.sql
        assert "node_role =" not in client.sql
        assert "IN" not in client.sql
        assert "positionCaseInsensitive" not in client.sql
        assert set(client.parameters) == {"from_", "to", "limit"}

    def test_노드와_역할은_바인딩_파라미터로_나간다(self):
        client = _FakeClient()

        _adapter(client).fetch_node_logs(
            _START, _END, node="es-data-02", node_role="master"
        )

        assert "node = %(node)s" in client.sql
        assert "node_role = %(node_role)s" in client.sql
        assert client.parameters["node"] == "es-data-02"
        assert client.parameters["node_role"] == "master"

    def test_레벨은_두_컬럼_모두_대문자로_비교한다(self):
        """level은 ES가 남긴 'WARN '(패딩), detected_level은 수집기가 추론한
        'warn'이다. 한쪽만 보면 수집기 설정이 바뀔 때 조용히 0건이 된다."""
        client = _FakeClient()

        _adapter(client).fetch_node_logs(_START, _END, levels=("warn", "Error"))

        assert "upper(trimBoth(level)) IN %(levels)s" in client.sql
        assert "upper(trimBoth(detected_level)) IN %(levels)s" in client.sql
        assert client.parameters["levels"] == ("ERROR", "WARN")

    def test_공백만_있는_레벨은_조건을_만들지_않는다(self):
        """빈 IN 절은 ClickHouse 문법 오류다."""
        client = _FakeClient()

        _adapter(client).fetch_node_logs(_START, _END, levels=("", "  "))

        assert "IN" not in client.sql
        assert "levels" not in client.parameters

    def test_키워드는_대소문자_무시_부분일치다(self):
        client = _FakeClient()

        _adapter(client).fetch_node_logs(_START, _END, keyword="OutOfMemory")

        assert "positionCaseInsensitive(line, %(keyword)s) > 0" in client.sql
        assert client.parameters["keyword"] == "OutOfMemory"

    def test_키워드를_SQL에_이어붙이지_않는다(self):
        """LLM이 주는 값이다. 이어 붙이면 그대로 주입 경로가 된다."""
        client = _FakeClient()
        injection = "' OR 1=1 --"

        _adapter(client).fetch_node_logs(_START, _END, node=injection, keyword=injection)

        assert injection not in client.sql
        assert client.parameters["node"] == injection


class TestLimit:
    def test_기본_상한을_넘기지_않는다(self):
        client = _FakeClient()

        _adapter(client).fetch_node_logs(_START, _END, limit=99_999)

        assert client.parameters["limit"] == MAX_NODE_LOG_LIMIT

    def test_음수_요청도_최소_1로_보정한다(self):
        client = _FakeClient()

        _adapter(client).fetch_node_logs(_START, _END, limit=-5)

        assert client.parameters["limit"] == 1

    def test_상한에_걸리면_경고를_남긴다(self, caplog):
        """정렬해서 자르므로 조용한 손실은 아니지만, 잘렸다는 사실은 알려야
        한다 — agent가 구간을 좁혀 다시 물어볼 근거가 된다."""
        client = _FakeClient(rows=[_ROW, _ROW])

        with caplog.at_level("WARNING"):
            _adapter(client).fetch_node_logs(_START, _END, limit=2)

        assert "node_log hit the row limit" in caplog.text


class TestSpanValidation:
    def test_역순_구간은_거부한다(self):
        with pytest.raises(InvalidTimeRangeError):
            _adapter(_FakeClient()).fetch_node_logs(_END, _START)

    def test_시간대가_섞이면_거부한다(self):
        """naive와 aware를 섞으면 비교가 TypeError로 터지고, 그 예외는 도메인
        거절이 아니라 내부 오류로 보고된다."""
        naive = datetime(2026, 9, 10, 2, 15)

        with pytest.raises(InvalidTimeRangeError):
            _adapter(_FakeClient()).fetch_node_logs(_START, naive)

    def test_10분을_넘는_구간도_허용한다(self):
        """TimeRange의 10분 상한은 분 단위 팬아웃 비용에서 온 것이다. 이
        조회는 단일 쿼리이고 비용이 limit으로 묶여 있어 해당하지 않는다."""
        client = _FakeClient()
        far_end = _START + timedelta(hours=3)

        entries = _adapter(client).fetch_node_logs(_START, far_end)

        assert len(entries) == 1


class TestLoggerFilter:
    """레벨만으로는 클러스터 사건을 못 잡는다.

    이 수집의 목적인 샤드 재배치·노드 이탈·allocation은 ES가 INFO로 남긴다.
    그렇다고 INFO를 열면 실측에서 10건 중 9건이 ML 유지보수·만료 데이터 삭제
    잡음이었다. 그래서 레벨과 로거를 OR로 묶는다.
    """

    def test_레벨과_로거는_서로_OR로_묶인다(self):
        client = _FakeClient()

        _adapter(client).fetch_node_logs(
            _START,
            _END,
            levels=("WARN", "ERROR"),
            loggers=("o.e.c.r.a.AllocationService",),
        )

        # 하나의 괄호 그룹 안에서 OR로 이어져야 한다. AND로 묶이면
        # "INFO인데 AllocationService인 줄"이 통째로 빠진다.
        assert (
            "(upper(trimBoth(level)) IN %(levels)s"
            " OR upper(trimBoth(detected_level)) IN %(levels)s"
            " OR trimBoth(logger) IN %(loggers)s)"
        ) in client.sql

    def test_저장된_logger의_공백_패딩을_벗겨_비교한다(self):
        """실측값이 "o.e.t.TransportService    "처럼 뒤에 공백을 달고 있다.
        trimBoth가 없으면 정확 일치가 전부 0건이 된다."""
        client = _FakeClient()

        _adapter(client).fetch_node_logs(_START, _END, loggers=("o.e.c.c.Coordinator",))

        assert "trimBoth(logger) IN %(loggers)s" in client.sql

    def test_넘어온_로거_이름의_공백도_정리한다(self):
        client = _FakeClient()

        _adapter(client).fetch_node_logs(
            _START, _END, loggers=("  o.e.c.c.Coordinator  ", "", "   ")
        )

        assert client.parameters["loggers"] == ("o.e.c.c.Coordinator",)

    def test_로거_이름은_대소문자를_바꾸지_않는다(self):
        """ES 클래스 이름이라 대소문자가 의미를 가진다. levels와 다르다."""
        client = _FakeClient()

        _adapter(client).fetch_node_logs(_START, _END, loggers=("o.e.c.s.MasterService",))

        assert client.parameters["loggers"] == ("o.e.c.s.MasterService",)

    def test_로거만_주면_레벨_조건은_만들지_않는다(self):
        client = _FakeClient()

        _adapter(client).fetch_node_logs(
            _START, _END, levels=(), loggers=("o.e.c.c.Coordinator",)
        )

        # SELECT 투영에는 level 컬럼이 그대로 있다. 사라져야 하는 것은
        # WHERE 절의 레벨 조건이다.
        assert "trimBoth(level)" not in client.sql
        assert "levels" not in client.parameters
        assert "trimBoth(logger) IN %(loggers)s" in client.sql

    def test_둘_다_비우면_조건_자체가_없다(self):
        client = _FakeClient()

        _adapter(client).fetch_node_logs(_START, _END, levels=(), loggers=())

        assert "IN" not in client.sql
        assert set(client.parameters) == {"from_", "to", "limit"}

    def test_로거는_바인딩_파라미터로_나간다(self):
        client = _FakeClient()

        _adapter(client).fetch_node_logs(_START, _END, loggers=("'; DROP TABLE x --",))

        assert "DROP TABLE" not in client.sql
