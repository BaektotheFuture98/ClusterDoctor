"""Presentation helpers for the report header and summary."""

from datetime import datetime

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import (
    NodeMetricRow,
    Observations,
)

DEMO_GAP = "디자인 미리보기용 가상 데이터입니다. 실제 장애 분석 결과가 아닙니다."
DEMO_NOTE = "실제 장애 분석 결과가 아닌 디자인 미리보기 데이터입니다."
MAX_KEY_OBSERVATIONS = 5


def is_demo(gaps: tuple[str, ...]) -> bool:
    return DEMO_GAP in gaps


def format_window(start: datetime, end: datetime) -> str:
    """`2026-10-01 14:02 ─ 14:12 KST`; the end repeats the date only when it differs."""
    s = start.astimezone(KST)
    e = end.astimezone(KST)
    end_text = e.strftime("%H:%M" if e.date() == s.date() else "%Y-%m-%d %H:%M")
    return f"{s.strftime('%Y-%m-%d %H:%M')} ─ {end_text} KST"


def confidence_label(value: str) -> str:
    upper = value.upper()
    return upper if upper in {"HIGH", "MEDIUM", "LOW"} else "—"


def key_observations(obs: Observations) -> tuple[tuple[str, str], ...]:
    """Up to five key/value pairs from nodes that actually reported samples."""
    nodes = [row for row in obs.nodes if row.samples > 0]
    if not nodes:
        return ()
    items: list[tuple[str, str]] = []

    def rejected(row: NodeMetricRow) -> int:
        return row.search_rejected_max + row.write_rejected_max

    def queue(row: NodeMetricRow) -> int:
        return max(row.search_queue_max, row.write_queue_max)

    impact = max(nodes, key=rejected)
    if rejected(impact) == 0:
        impact = max(nodes, key=queue)
        if queue(impact) == 0:
            impact = None
    if impact:
        items.append(("영향 노드", impact.node))
    items.append(("Search queue max", str(max(r.search_queue_max for r in nodes))))
    items.append(("Search rejected", str(sum(r.search_rejected_max for r in nodes))))
    write_rejected = sum(r.write_rejected_max for r in nodes)
    write_queue = max(r.write_queue_max for r in nodes)
    if write_rejected == 0 and write_queue > 0:
        items.append(("Write queue max", str(write_queue)))
    else:
        items.append(("Write rejected", str(write_rejected)))
    items.append(("JVM heap max", f"{max(r.jvm_heap_max for r in nodes)}%"))
    return tuple(items[:MAX_KEY_OBSERVATIONS])
