"""마스터 로그의 클러스터 상태 이벤트(health 전환·샤드 실패·노드 이탈)를 코드가 센다.

원인 판정이 아니라 사실과 시간 정렬만 만든다. 느린 분과 느리지 않은 분 양쪽에서 몇 분에 나타났는지 함께 남긴다.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime

from cluster_doctor.incident_analysis_agent.model.observations import MasterEvent

_HEALTH = re.compile(r"Cluster health status changed from \[(\w+)\] to \[(\w+)\]")
_SHARD_MARKS = ("marking unavailable shards", "failing shard", "shard-failed", "marking and sending shard failed")
_NODE_LEFT_MARKS = ("node-left", "removed {")
KIND_ORDER = ("health", "shard", "node_left")


@dataclass(frozen=True)
class ClusterEventKind:
    kind: str
    count: int
    first: datetime | None
    last: datetime | None
    # health만 쓴다. (이전 상태, 이후 상태, 건수).
    transitions: tuple[tuple[str, str, int], ...] = ()


@dataclass(frozen=True)
class ClusterEvents:
    kinds: tuple[ClusterEventKind, ...] = ()
    slow_minutes: int = 0
    slow_minutes_with_events: int = 0
    other_minutes: int = 0
    other_minutes_with_events: int = 0


def _classify(event: MasterEvent) -> tuple[str, tuple[str, str] | None] | None:
    text = event.line or event.rendered
    if m := _HEALTH.search(text):
        return "health", (m.group(1), m.group(2))
    if any(mark in text for mark in _SHARD_MARKS):
        return "shard", None
    if any(mark in text for mark in _NODE_LEFT_MARKS):
        return "node_left", None
    return None


def parse_cluster_events(events: tuple[MasterEvent, ...], minutes: tuple) -> ClusterEvents:
    """minutes는 correlation의 MinuteLink 목록(slow_queries, minute 속성)."""
    counts: Counter[str] = Counter()
    stamps: dict[str, list[datetime]] = {}
    transitions: Counter[tuple[str, str]] = Counter()
    event_minutes: set[datetime] = set()
    for event in events:
        found = _classify(event)
        if found is None:
            continue
        kind, move = found
        counts[kind] += 1
        if move:
            transitions[move] += 1
        if event.timestamp is not None:
            stamps.setdefault(kind, []).append(event.timestamp)
            event_minutes.add(event.timestamp.replace(second=0, microsecond=0))
    kinds = tuple(
        ClusterEventKind(
            kind, counts[kind], min(stamps[kind], default=None) if kind in stamps else None,
            max(stamps[kind]) if kind in stamps else None,
            tuple((a, b, n) for (a, b), n in transitions.most_common()) if kind == "health" else (),
        )
        for kind in KIND_ORDER if counts[kind]
    )
    slow = {m.minute for m in minutes if m.slow_queries > 0}
    other = {m.minute for m in minutes if m.slow_queries == 0}
    return ClusterEvents(kinds, len(slow), len(slow & event_minutes), len(other), len(other & event_minutes))
