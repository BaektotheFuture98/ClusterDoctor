"""노드 리소스 메트릭 항목.

한 시점의 노드 상태를 스냅샷한 측정값이다. 사건(event) 로그인 SlowlogEntry·
QueryLogEntry와 달리 주기적으로 수집되는 샘플이므로 별도 파일에 둔다.

ClickHouse의 ``es_node_metric`` 테이블에서만 온다(테이블 이름과 달리
Elasticsearch가 아니라 ClickHouse에 적재된다 — ``settings.py``의
``clickhouse_node_metric_table`` 참고).
"""

from dataclasses import dataclass
from typing import ClassVar

from cluster_doctor.domain.model.log_entry import LogEntry


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
