"""세 소스가 공유하는 계약.

개별 사건(event) 로그(slowlog / es_query_log / node_log)와 주기 수집 샘플
(node_metric)이 전부 이 베이스를 쓴다. 실제 타입은 소스별 폴더에 있다 —
``kafka/slowlog_entry.py``, ``clickhouse/node_log_entry.py``,
``clickhouse/query_log_entry.py``, ``clickhouse/node_metric_entry.py``.

공통으로 두는 것은 발생 시각과 출처뿐이다. ``fetch_logs``가 여럿을 한 리스트에
담아 돌려주고 ``split_by_minute``이 ``timestamp``로 묶으므로 그 둘은 필요하다.
``source``는 인스턴스 필드가 아니라 ``ClassVar``다 — 타입이 정해지면 출처도
정해지므로 생성자에서 매번 넘길 이유가 없고, 넘기면 오타 한 번에 프롬프트의
소스별 묶기가 조용히 어긋난다.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import ClassVar


@dataclass(frozen=True)
class LogEntry:
    """세 소스가 공유하는 계약."""

    timestamp: datetime

    # 값을 주지 않는다. 구현체가 반드시 정의해야 하는 것이지, 빠뜨렸을 때
    # 조용히 넘어갈 기본값이 있어서는 안 된다.
    source: ClassVar[str]


@dataclass(frozen=True)
class NodeLogEntry(LogEntry):
    """ES 노드가 남긴 로그 한 줄.

    같은 내용을 SSH로도 가져올 수 있지만(데이터 노드는 아직 그 경로뿐이다),
    ClickHouse 조회가 세 가지에서 낫다.

      - ``node_role``이 있어 마스터 노드를 이름 없이 지목할 수 있다.
        SSH 경로는 ES에 ``_master``를 물어 IP를 얻는 왕복이 필요했다.
      - ``level``·``detected_level``이 컬럼이라 severity 정규식이 필요 없다.
      - 여러 노드를 한 쿼리로 본다. SSH는 노드마다 접속을 새로 열었다.

    ``level``은 ES가 기록한 값이고 ``detected_level``은 수집기가 추론한 값이다.
    둘이 어긋날 수 있으므로(``"WARN "`` 대 ``"warn"``) 둘 다 들고 있는다 —
    어느 쪽을 신뢰할지는 조회하는 쪽이 정한다.

    SSH 경로(``domain/model/ssh``)는 이 타입을 만들지 않는다 — 원문 문자열을
    그대로 돌려준다.
    """

    source: ClassVar[str] = "node_log"

    node: str
    node_role: str
    level: str
    detected_level: str
    logger: str
    filename: str
    host: str
    # 로그 원문 한 줄. 파싱하지 않는다 — 스택 트레이스·GC 통계처럼 구조가
    # 제각각인 내용이 들어오고, 진단에 쓰이는 것은 문장 그대로다.
    line: str


@dataclass(frozen=True)
class NodeMetricEntry(LogEntry):
    """한 노드의 한 시점 리소스 샘플."""

    source: ClassVar[str] = "node_metric"

    node_name: str
    node_ip: str
    os_cpu_percent: int
    os_mem_used_percent: int
    process_cpu_percent: int
    jvm_heap_used_percent: int
    search_active: int
    search_queue: int
    search_rejected: int
    write_active: int
    write_queue: int
    write_rejected: int


@dataclass(frozen=True)
class QueryLogEntry(LogEntry):
    """ES 쿼리 실행 기록 한 건. company·user가 사는 곳이다."""

    source: ClassVar[str] = "es_query_log"

    host: str
    run_time: Decimal
    success: bool
    cmd: str
    service: str
    env: str
    project: str
    cluster: str
    # tuple이다. list를 필드로 두면 frozen이어도 해시가 깨져 중복 제거·집합
    # 연산에 쓸 수 없다.
    keywords: tuple[str, ...]
    company: str | None
    user: str | None


@dataclass(frozen=True)
class SlowlogEntry(LogEntry):
    """ES가 임계치 초과로 남긴 느린 쿼리 한 건.

    ``timestamp``를 뺀 나머지에 기본값을 둔 이유: Kafka consumer는 트리거용으로
    도착 시각만 아는 항목을 만든다(메시지 파싱에 실패하면 그마저도 수신 시각으로
    폴백한다). ClickHouse 경로만 전부를 채운다 — 그래서 이 타입은 kafka와
    clickhouse 두 소스가 함께 채운다. 어느 한쪽 폴더에만 있을 수 없는 타입이라
    유입 지점인 kafka에 둔다.
    """

    source: ClassVar[str] = "slowlog"

    index_name: str = ""
    node: str = ""
    took: str = ""
    total_hits: str = ""
    total_shards: int = 0
    # x-opaque-id 원문. "service=web,project=...,company=50,user=579,..." 형태이며
    # company/user가 여기 숫자 ID로 들어 있다 — es_query_log의 표시명과는 다른
    # 식별자 체계라 아직 필드로 분리하지 않는다.
    opaque_id: str = ""
    query: str = ""
