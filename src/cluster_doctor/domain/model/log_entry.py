"""소스별 로그 항목.

세 소스(slowlog / es_query_log / node_metric)는 담는 것이 서로 다르다. 하나의
``LogEntry``로 합쳐 쓰는 동안 두 가지가 무너져 있었다.

  - 같은 필드가 소스마다 다른 뜻이었다. ``component``가 slowlog에서는 인덱스명,
    es_query_log에서는 service, node_metric에서는 ``None``이었다. 이름만 보고는
    무엇이 들었는지 알 수 없었다.
  - 값이 ``message`` 문자열로 평탄화됐다. ``run_time``·``keywords``·``cpu``가
    전부 텍스트가 되어, 느린 쿼리만 거르거나 keyword 배열을 자르려면 만들어 둔
    문자열을 다시 파싱하는 수밖에 없었다.

공통으로 두는 것은 발생 시각과 출처뿐이다. ``fetch_logs``가 셋을 한 리스트에
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
class SlowlogEntry(LogEntry):
    """ES가 임계치 초과로 남긴 느린 쿼리 한 건.

    ``timestamp``를 뺀 나머지에 기본값을 둔 이유: Kafka consumer는 트리거용으로
    도착 시각만 아는 항목을 만든다(메시지 파싱에 실패하면 그마저도 수신 시각으로
    폴백한다). ClickHouse 경로만 전부를 채운다.
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
