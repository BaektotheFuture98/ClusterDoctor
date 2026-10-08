"""노드 메트릭 datasource: ClickHouse 조회 + Evidence 규칙. **LLM을 부르지 않는다.**

메트릭은 숫자다. "heap이 92%였다"는 판단이 아니라 측정이고, 측정을 모델에게
읽히면 옮겨 적다 틀린다. 그래서 이 소스만 분 단위 선별 그래프를 타지 않고
규칙으로 Evidence를 만든다.

규칙은 임계값 셋뿐이고 전부 구조화된 필드에서 나온다. 임계값을 넘지 않으면
Evidence를 만들지 않는다 — 정상 구간의 평범한 heap 수치가 Cross-source 입력에
쌓이면, 근거 목록이 배경 소음으로 채워진다.

노드·규칙마다 **최고점 한 건만** 남긴다. 5초 간격 샘플을 그대로 실으면 10분
구간에서 노드 하나가 120건을 낸다.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from cluster_doctor.incident_analysis_agent.datasource.clickhouse.client import (
    MAX_ROWS_PER_SEGMENT_PER_SOURCE,
    query_segment,
)
from cluster_doctor.incident_analysis_agent.model.evidence import (
    Evidence,
    EvidenceProvenance,
    EvidenceSource,
)
from cluster_doctor.incident_analysis_agent.model.log_entries import (
    LogEntry,
    NodeMetricEntry,
)
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange

# rejected는 기동 이후 누적 카운터라 값이 있어도 현재 window의 장애 증거가 아니다.
_REJECTED_FLOOR = 0


@dataclass(frozen=True)
class NodeMetricThresholds:
    """이 클러스터에서 무엇을 이상으로 볼 것인가.

    상수가 아니라 값인 이유: heap 사이징과 thread pool 크기가 클러스터마다
    다르다. 8GB heap에서 85%는 경고지만 64GB heap에서는 평상시일 수 있고,
    그 차이를 코드에 박아 두면 한쪽 클러스터에서 배경 소음이 근거 목록을
    채운다.

    기본값은 ES의 일반적인 설정에서 나온다 — circuit breaker가 95%이고
    old-gen GC 압력이 눈에 띄기 시작하는 지점이 85% 근처, search queue 기본
    크기가 1000이므로 100이면 이미 밀리고 있다는 뜻이다.
    """

    heap_warn_percent: int = 85
    queue_warn: int = 100


DEFAULT_THRESHOLDS = NodeMetricThresholds()


def fetch(client, table: str, tr: TimeRange) -> list[LogEntry]:
    sql = (
        f"SELECT reg_date, node_name, node_ip, os_cpu_percent, os_mem_used_percent, "
        "process_cpu_percent, jvm_heap_used_percent, "
        "search_active, search_queue, search_rejected, "
        f"write_active, write_queue, write_rejected FROM {table} "
        "WHERE reg_date >= %(from_)s AND reg_date < %(to)s "
        f"LIMIT {MAX_ROWS_PER_SEGMENT_PER_SOURCE}"
    )
    provenance = EvidenceProvenance(
        method="clickhouse",
        collected_at=datetime.now(UTC),
        table=table,
        query_from=tr.start,
        query_to=tr.end,
    )
    return [
        NodeMetricEntry(
            timestamp=row[0],
            provenance=provenance,
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
        for row in query_segment(client, sql, tr, "node_metric")
    ]


def to_evidence(
    entries: list[NodeMetricEntry],
    *,
    new_evidence_id: Callable[[], str],
    thresholds: NodeMetricThresholds = DEFAULT_THRESHOLDS,
) -> list[Evidence]:
    """임계를 넘은 노드 메트릭만 Evidence로."""
    peaks: dict[tuple[str, str], tuple[NodeMetricEntry, int, str, str]] = {}

    def consider(
        entry: NodeMetricEntry, rule: str, value: int, severity: str, message: str
    ) -> None:
        key = (entry.node_name, rule)
        current = peaks.get(key)
        if current is None or value > current[1]:
            peaks[key] = (entry, value, severity, message)

    for entry in entries:
        rejected = entry.search_rejected + entry.write_rejected
        if rejected > _REJECTED_FLOOR:
            consider(
                entry,
                "rejected",
                rejected,
                "Info",
                f"{entry.node_name} search_rejected={entry.search_rejected} "
                f"write_rejected={entry.write_rejected} (누적 카운터, 구간 증가분 아님)",
            )
        if entry.jvm_heap_used_percent >= thresholds.heap_warn_percent:
            consider(
                entry,
                "heap",
                entry.jvm_heap_used_percent,
                "Warning",
                f"{entry.node_name} jvm_heap={entry.jvm_heap_used_percent}% "
                f"(기준 {thresholds.heap_warn_percent}%) "
                f"cpu={entry.os_cpu_percent}% mem={entry.os_mem_used_percent}%",
            )
        if max(entry.search_queue, entry.write_queue) >= thresholds.queue_warn:
            consider(
                entry,
                "queue",
                max(entry.search_queue, entry.write_queue),
                "Warning",
                f"{entry.node_name} search_queue={entry.search_queue} "
                f"write_queue={entry.write_queue} (기준 {thresholds.queue_warn}) "
                f"search_active={entry.search_active}",
            )

    evidence: list[Evidence] = []
    for (node_name, rule), (entry, _value, severity, message) in peaks.items():
        evidence.append(
            Evidence(
                evidence_id=new_evidence_id(),
                event_time=entry.timestamp,
                source=EvidenceSource.NODE_METRIC,
                node_name=node_name,
                event_type=f"node_metric_{rule}",
                severity=severity,
                message=message,
                provenance=entry.provenance,
                selection_reason="임계값을 넘은 구간 최고점. 코드가 측정값에서 직접 골랐다.",
            )
        )
    evidence.sort(key=lambda item: item.event_time)
    return evidence
