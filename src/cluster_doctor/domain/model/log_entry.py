"""이벤트 로그 항목.

두 소스(slowlog / es_query_log)의 개별 사건 기록이다. 주기 수집 샘플인
NodeMetricEntry는 node_metric.py에 따로 둔다.

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
class NodeLogEntry(LogEntry):
    """ES 노드가 남긴 로그 한 줄.

    예전에는 이 정보를 노드에 SSH로 붙어 파일에서 긁어왔다. 같은 내용이
    ClickHouse에 적재되면서 조회로 바뀌었고, 덕분에 세 가지가 달라졌다.

      - ``node_role``이 있어 마스터 노드를 이름 없이 지목할 수 있다.
        SSH 경로는 ES에 ``_master``를 물어 IP를 얻는 왕복이 필요했다.
      - ``level``·``detected_level``이 컬럼이라 severity 정규식이 필요 없다.
      - 여러 노드를 한 쿼리로 본다. SSH는 노드마다 접속을 새로 열었다.

    ``level``은 ES가 기록한 값이고 ``detected_level``은 수집기가 추론한 값이다.
    둘이 어긋날 수 있으므로(``"WARN "`` 대 ``"warn"``) 둘 다 들고 있는다 —
    어느 쪽을 신뢰할지는 조회하는 쪽이 정한다.
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
