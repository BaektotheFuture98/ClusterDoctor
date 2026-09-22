"""Kafka slowlog 트리거 메시지에서 실제로 쓰는 값 하나.

Kafka 메시지의 목적은 발생 시각을 알려 분석할 Incident를 트리거하는 것뿐이다.
발생 시각 상세(인덱스명·소요 시간·노드 등)는 ClickHouse가 갖고 있고,
``EvidenceCollector``가 트리거 이후 직접 조회한다.

ClickHouse에서 오는 상세 ``SlowlogEntry``(``agent.integrations.clickhouse.models``)를
여기서 재사용하지 않는 이유: 그 타입은 필드 7개 중 6개가 트리거 목적에는 없는
값이라 ``query=""``, ``total_shards=0``처럼 항상 기본값으로만 채워진다. 트리거
전용 타입을 따로 두면 그 여섯 기본값이 사라지고, "이 값이 실제로 관측된 0인지
아니면 애초에 채우지 않은 것인지"를 헷갈릴 자리가 생기지 않는다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class SlowlogTriggerEvent:
    """Kafka에서 받은 slowlog 트리거 한 건. 발생 시각만 안다."""

    timestamp: datetime
