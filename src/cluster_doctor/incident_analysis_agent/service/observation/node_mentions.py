"""마스터 로그 줄에서 노드 ID가 나온 곳을 코드로 뽑는다.

데이터 노드에 SSH로 붙는 유일한 계기는 "수집한 마스터 로그에 그 노드의 ID가
적혀 있다"이다. 모델의 판단이 아니라 줄의 내용이 기준이므로, LLM이 고른
근거가 아니라 구간의 마스터 로그 전체를 본다.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

from cluster_doctor.incident_analysis_agent.model.observations import MasterEvent
from cluster_doctor.incident_analysis_agent.service.observation.master_wait import (
    _NODE,
)

# ES 노드 ID는 22자 base64url이다. 이름·호스트명 같은 짧은 토큰을 거른다.
_NODE_ID_SHAPE = re.compile(r"^[A-Za-z0-9_-]{20,24}$")
_NODE_BRACKET = re.compile(r"node \[([A-Za-z0-9_-]+)\]")

_SEVERITY = {"WARN": 1, "WARNING": 1, "ERROR": 2, "FATAL": 2}


@dataclass(frozen=True)
class NodeMention:
    node_id: str
    node_name: str = ""
    ip: str = ""
    count: int = 0
    # 이 노드가 나온 줄 중 가장 높은 수준. "ERROR"/"WARN" 또는 빈 문자열.
    worst_level: str = ""

    @property
    def severity(self) -> int:
        return _SEVERITY.get(self.worst_level, 0)


def _mentions_in_line(text: str) -> dict[str, tuple[str, str]]:
    """한 줄에 나온 {노드 ID: (이름, ip)}. 같은 ID는 한 번만 센다."""
    found: dict[str, tuple[str, str]] = {}
    for match in _NODE.finditer(text):
        node_id = match.group(2).strip()
        if _NODE_ID_SHAPE.match(node_id):
            found[node_id] = (match.group(1).strip(), match.group(3).strip())
    for match in _NODE_BRACKET.finditer(text):
        node_id = match.group(1)
        if _NODE_ID_SHAPE.match(node_id):
            found.setdefault(node_id, ("", ""))
    return found


def node_mentions(events: Iterable[MasterEvent]) -> tuple[NodeMention, ...]:
    """마스터 로그 줄 전체에서 노드 ID 언급을 모은다. 첫 등장 순서를 지킨다."""
    merged: dict[str, NodeMention] = {}
    for event in events:
        level = (event.level or "").strip().upper()
        for node_id, (name, ip) in _mentions_in_line(event.line or event.rendered).items():
            prev = merged.get(node_id) or NodeMention(node_id)
            worst = level if _SEVERITY.get(level, 0) > prev.severity else prev.worst_level
            merged[node_id] = NodeMention(
                node_id,
                prev.node_name or name,
                prev.ip or ip,
                prev.count + 1,
                worst,
            )
    return tuple(merged.values())
