from dataclasses import dataclass
from typing import ClassVar

from cluster_doctor.domain.model.log_entry import LogEntry


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
