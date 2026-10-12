"""마스터 로그 한 줄에서 응답 대기 구간과 대상 노드를 읽는다.

로그 시각은 마스터가 늦은 응답을 받은 시각이다. 요청을 보낸 시각은
``sent [13.4s/13415ms] ago``로 거슬러 계산한다. 쿼리와 겹치는지 볼 대상은
로그 시각이 아니라 이 대기 구간이다.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

from cluster_doctor.incident_analysis_agent.model.observations import MasterEvent

_SENT = re.compile(r"sent \[[^/\]]+/(\d+)ms\] ago")
_TIMED_OUT = re.compile(r"timed out \[[^/\]]+/(\d+)ms\] ago")
# stats[n]처럼 action 안에 대괄호가 들어간다. 다음 필드 ", node [" 앞까지 읽는다.
_ACTION = re.compile(r"action \[(.+?)\], node \[")
# ES 노드 표기: {name}{nodeId}{ephemeralId}{hostName}{hostAddress}{address:port}...
_NODE = re.compile(r"node \[\{([^}]*)\}\{([^}]*)\}\{[^}]*\}\{[^}]*\}\{([^}]*)\}")
_NODE_ID_ONLY = re.compile(r"for node \[([A-Za-z0-9_-]+)\]")


@dataclass(frozen=True)
class MasterWait:
    event: MasterEvent
    action: str
    target_node: str
    target_node_id: str
    target_ip: str
    sent_at: datetime | None
    received_at: datetime | None
    timeout_ms: int | None


def parse_master_wait(event: MasterEvent) -> MasterWait | None:
    text = event.line or event.rendered
    node = _NODE.search(text)
    node_id = _NODE_ID_ONLY.search(text)
    if node is None and node_id is None:
        return None
    sent = _SENT.search(text)
    timed_out = _TIMED_OUT.search(text)
    action = _ACTION.search(text)
    sent_ms = int(sent.group(1)) if sent else None
    timed_out_ms = int(timed_out.group(1)) if timed_out else None
    has_window = event.timestamp is not None and sent_ms is not None
    return MasterWait(
        event=event,
        action=action.group(1) if action else event.logger,
        target_node=node.group(1) if node else "",
        target_node_id=node.group(2) if node else node_id.group(1),
        target_ip=node.group(3) if node else "",
        sent_at=event.timestamp - timedelta(milliseconds=sent_ms) if has_window else None,
        received_at=event.timestamp if has_window else None,
        timeout_ms=sent_ms - timed_out_ms if sent_ms is not None and timed_out_ms is not None else None,
    )


def parse_master_waits(events: Iterable[MasterEvent]) -> tuple[MasterWait, ...]:
    """id만 남긴 줄(``failed to retrieve stats for node [id]``)은 같은 묶음에서 이름을 찾는다."""
    waits = [wait for event in events if (wait := parse_master_wait(event)) is not None]
    known = {w.target_node_id: w for w in waits if w.target_node}
    return tuple(
        replace(w, target_node=known[w.target_node_id].target_node, target_ip=known[w.target_node_id].target_ip)
        if not w.target_node and w.target_node_id in known else w
        for w in waits
    )
