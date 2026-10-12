"""소스 사이의 시간·노드 연결. 모델이 아니라 코드가 대조한다.

쿼리 로그 시각(``reg_date``)이 요청 시작인지 종료인지 확정되지 않았다. 그래서
겹침은 두 가정으로 각각 계산하고, 두 가정 모두에서 겹친 것과 한 가정에서만
겹친 것을 나눠 둔다. 한쪽 가정의 결과를 사실처럼 싣지 않기 위해서다.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from statistics import median
from typing import Literal

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry, query_record_key
from cluster_doctor.incident_analysis_agent.model.observations import (
    NodeMetricRow,
    Observations,
    SourceWindowStatus,
)
from cluster_doctor.incident_analysis_agent.service.observation.master_wait import (
    MasterWait,
    parse_master_waits,
)
from cluster_doctor.incident_analysis_agent.service.observation.query_requests import valid_runtime

SLOW_QUERY_SECONDS = Decimal("10")
_PROFILE_ROWS = 8
_TIER = re.compile(r"tier=([^,}\]]+)")
# 느린 형태가 아닌 요청의 중앙값이 기준 분보다 이 배수를 넘게 늘면 전반 지연으로 본다.
_SPREAD_RATIO = Decimal("2")
_MIN_SAMPLES = 20
# 기준 중앙값이 0에 가까우면 아주 작은 변화도 배수로는 커진다. 바닥값을 둔다.
_MEDIAN_FLOOR = Decimal("0.05")


@dataclass(frozen=True)
class QueryInterval:
    record_key: str
    logged_at: datetime
    seconds: Decimal
    cmd: str
    user: str
    target_host: str

    def if_logged_at_end(self) -> tuple[datetime, datetime]:
        return self.logged_at - timedelta(seconds=float(self.seconds)), self.logged_at

    def if_logged_at_start(self) -> tuple[datetime, datetime]:
        return self.logged_at, self.logged_at + timedelta(seconds=float(self.seconds))


@dataclass(frozen=True)
class WaitOverlap:
    wait: MasterWait
    both: tuple[QueryInterval, ...]
    one_assumption: tuple[QueryInterval, ...]


@dataclass(frozen=True)
class MinuteLink:
    minute: datetime
    slow_queries: int
    master_waits: int


@dataclass(frozen=True)
class NodeCard:
    node: str
    node_ip: str
    tier: str
    waits: tuple[MasterWait, ...]
    metrics: NodeMetricRow | None
    node_log: tuple[SourceWindowStatus, ...]
    queries_targeting: int


@dataclass(frozen=True)
class QueryProfileRow:
    cmd: str
    date_range: int
    index_count: int
    total: int
    slow: int
    median_seconds: Decimal | None
    max_seconds: Decimal | None


@dataclass(frozen=True)
class Verdict:
    """지연이 특정 형태의 요청에 한정됐는가, 같은 시각 다른 요청도 느려졌는가.

    기준은 과거가 아니라 같은 분석 구간 안의 "느린 요청이 없던 분"이다.
    """

    scope: Literal["specific", "cluster_wide", "undetermined", "no_slow"]
    slow_shapes: tuple[tuple[str, int, int], ...]
    reference_median: Decimal | None
    slow_minute_median: Decimal | None


@dataclass(frozen=True)
class CrossLinks:
    slow_threshold: Decimal
    overlaps: tuple[WaitOverlap, ...]
    minutes: tuple[MinuteLink, ...]
    nodes: tuple[NodeCard, ...]
    profile: tuple[QueryProfileRow, ...]
    verdict: Verdict


def _host(value: str | None) -> str:
    return (value or "").split(":")[0].strip()


def _minute(moment: datetime) -> datetime:
    return moment.replace(second=0, microsecond=0)


def _median(values: list[Decimal]) -> Decimal | None:
    return Decimal(str(median(values))) if values else None


def _overlaps(left: tuple[datetime, datetime], right: tuple[datetime, datetime]) -> bool:
    return left[0] < right[1] and right[0] < left[1]


def query_shape(record: QueryLogEntry) -> tuple[str, int, int]:
    """키워드가 아니라 요청의 무게를 정하는 값으로만 묶는다."""
    index_count = len([name for name in (record.index_name or "").split(",") if name.strip()])
    return record.cmd, record.date_range, index_count


def _slow_intervals(observations: Observations, slow_seconds: Decimal) -> tuple[QueryInterval, ...]:
    rows = []
    for record in observations.query_requests:
        seconds = valid_runtime(record.run_time)
        if seconds is None or seconds < slow_seconds:
            continue
        rows.append(QueryInterval(query_record_key(record), record.timestamp, seconds,
                                  record.cmd, record.user, _host(record.target_host)))
    return tuple(sorted(rows, key=lambda q: q.logged_at))


def _wait_overlaps(waits: tuple[MasterWait, ...], slow: tuple[QueryInterval, ...]) -> tuple[WaitOverlap, ...]:
    result = []
    for wait in waits:
        if wait.sent_at is None or wait.received_at is None:
            continue
        window = (wait.sent_at, wait.received_at)
        at_end = {q.record_key for q in slow if _overlaps(q.if_logged_at_end(), window)}
        at_start = {q.record_key for q in slow if _overlaps(q.if_logged_at_start(), window)}
        result.append(WaitOverlap(
            wait=wait,
            both=tuple(q for q in slow if q.record_key in at_end & at_start),
            one_assumption=tuple(q for q in slow if q.record_key in at_end ^ at_start),
        ))
    return tuple(result)


def _minute_links(observations: Observations, waits: tuple[MasterWait, ...],
                  slow: tuple[QueryInterval, ...]) -> tuple[MinuteLink, ...]:
    slow_by_minute: dict[datetime, int] = defaultdict(int)
    for query in slow:
        slow_by_minute[_minute(query.logged_at)] += 1
    waits_by_minute: dict[datetime, int] = defaultdict(int)
    for wait in waits:
        if wait.event.timestamp is not None:
            waits_by_minute[_minute(wait.event.timestamp)] += 1
    minutes = {row.minute for row in observations.timeline} | set(slow_by_minute) | set(waits_by_minute)
    return tuple(MinuteLink(m, slow_by_minute.get(m, 0), waits_by_minute.get(m, 0)) for m in sorted(minutes))


def _node_cards(observations: Observations, waits: tuple[MasterWait, ...]) -> tuple[NodeCard, ...]:
    grouped: dict[str, list[MasterWait]] = defaultdict(list)
    for wait in waits:
        if wait.target_node:
            grouped[wait.target_node].append(wait)
    metrics = {row.node: row for row in observations.nodes}
    cards = []
    for node, node_waits in grouped.items():
        row = metrics.get(node)
        ip = node_waits[0].target_ip or (row.node_ip if row else "")
        tier = next((m.group(1) for w in node_waits if (m := _TIER.search(w.event.line or ""))), "")
        cards.append(NodeCard(
            node=node,
            node_ip=ip,
            tier=tier,
            waits=tuple(node_waits),
            metrics=row,
            node_log=tuple(s for s in observations.source_statuses
                           if s.source == "node_log" and s.host and s.host in (ip, node)),
            queries_targeting=sum(1 for r in observations.query_requests if ip and _host(r.target_host) == ip),
        ))
    return tuple(sorted(cards, key=lambda card: card.node))


def _profile(observations: Observations, slow_seconds: Decimal) -> tuple[QueryProfileRow, ...]:
    groups: dict[tuple[str, int, int], list[Decimal | None]] = defaultdict(list)
    for record in observations.query_requests:
        groups[query_shape(record)].append(valid_runtime(record.run_time))
    rows = []
    for (cmd, date_range, index_count), values in groups.items():
        measured = [v for v in values if v is not None]
        rows.append(QueryProfileRow(
            cmd=cmd, date_range=date_range, index_count=index_count, total=len(values),
            slow=sum(1 for v in measured if v >= slow_seconds),
            median_seconds=_median(measured),
            max_seconds=max(measured) if measured else None,
        ))
    rows.sort(key=lambda r: (-r.slow, -(r.max_seconds or 0), -r.total, r.cmd))
    return tuple(rows[:_PROFILE_ROWS])


def judge_spread(observations: Observations, *, slow_seconds: Decimal = SLOW_QUERY_SECONDS) -> Verdict:
    measured = [(r, v) for r in observations.query_requests if (v := valid_runtime(r.run_time)) is not None]
    slow_shapes = tuple(sorted({query_shape(r) for r, v in measured if v >= slow_seconds}))
    if not slow_shapes:
        return Verdict("no_slow", (), None, None)
    slow_minutes = {_minute(r.timestamp) for r, v in measured if v >= slow_seconds}
    others = [(r, v) for r, v in measured if query_shape(r) not in slow_shapes]
    reference = [v for r, v in others if _minute(r.timestamp) not in slow_minutes]
    during = [v for r, v in others if _minute(r.timestamp) in slow_minutes]
    reference_median, during_median = _median(reference), _median(during)
    if len(reference) < _MIN_SAMPLES or len(during) < _MIN_SAMPLES:
        return Verdict("undetermined", slow_shapes, reference_median, during_median)
    widened = during_median > max(reference_median, _MEDIAN_FLOOR) * _SPREAD_RATIO
    return Verdict("cluster_wide" if widened else "specific", slow_shapes, reference_median, during_median)


def build_cross_links(observations: Observations, *, slow_seconds: Decimal = SLOW_QUERY_SECONDS) -> CrossLinks:
    waits = parse_master_waits(observations.master_events)
    slow = _slow_intervals(observations, slow_seconds)
    return CrossLinks(
        slow_threshold=slow_seconds,
        overlaps=_wait_overlaps(waits, slow),
        minutes=_minute_links(observations, waits, slow),
        nodes=_node_cards(observations, waits),
        profile=_profile(observations, slow_seconds),
        verdict=judge_spread(observations, slow_seconds=slow_seconds),
    )


_PAYLOAD_LIST_LIMIT = 10
# 한 번의 분석은 최대 60분이므로 분 단위 목록의 상한도 같다.
_PAYLOAD_MINUTE_LIMIT = 60


def _iso(moment: datetime | None) -> str | None:
    return moment.astimezone(KST).isoformat() if moment else None


def _text(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def _node_priority(card: NodeCard) -> tuple:
    # 상한에 걸려도 실제 늦은 응답(sent_at이 있는 대기)이 있는 노드가 먼저 남도록 정렬한다.
    return (not any(w.sent_at for w in card.waits), -len(card.waits), card.node)


def cross_links_payload(links: CrossLinks, evidence_ids: dict[str, str]) -> dict:
    """모델 입력용 투영. 근거 id가 있는 쿼리는 id를 같이 실어 인용할 수 있게 한다.

    목록을 상한으로 자르면 그 건수를 ``omitted``에 남겨, 잘린 것이 없는 것으로 읽히지 않게 한다.
    """
    omitted: dict[str, int] = {}

    def cut(name: str, items: list | tuple, limit: int) -> list:
        if len(items) > limit:
            omitted[name] = omitted.get(name, 0) + len(items) - limit
        return list(items[:limit])

    def interval(query: QueryInterval) -> dict:
        return {"evidence_id": evidence_ids.get(query.record_key), "logged_at": _iso(query.logged_at),
                "execution_seconds": str(query.seconds), "cmd": query.cmd}

    def overlap(o: WaitOverlap) -> dict:
        return {
            "master_event_time": _iso(o.wait.event.timestamp),
            "target_node": o.wait.target_node or o.wait.target_node_id,
            "action": o.wait.action,
            "waiting_from": _iso(o.wait.sent_at),
            "waiting_to": _iso(o.wait.received_at),
            "timeout_ms": o.wait.timeout_ms,
            "overlap_under_both_assumptions": [
                interval(q) for q in cut("overlap_under_both_assumptions", o.both, _PAYLOAD_LIST_LIMIT)],
            "overlap_under_one_assumption": [
                interval(q) for q in cut("overlap_under_one_assumption", o.one_assumption, _PAYLOAD_LIST_LIMIT)],
        }

    def node(c: NodeCard) -> dict:
        return {
            "node": c.node, "node_ip": c.node_ip, "tier": c.tier, "master_waits": len(c.waits),
            "metrics_max": ({"cpu": c.metrics.cpu_max, "jvm_heap": c.metrics.jvm_heap_max,
                             "search_queue": c.metrics.search_queue_max} if c.metrics else None),
            "node_log": [{"status": s.status, "row_count": s.row_count, "error": s.error} for s in c.node_log],
            "queries_targeting_node": c.queries_targeting,
        }

    verdict = links.verdict
    payload = {
        "query_time_basis": "쿼리 로그 reg_date가 시작/종료 중 무엇인지 확정되지 않아 두 가정을 모두 계산했다",
        "slow_threshold_seconds": str(links.slow_threshold),
        "verdict": {
            "scope": verdict.scope,
            "slow_shapes": [{"cmd": c, "date_range": d, "index_count": n} for c, d, n in verdict.slow_shapes],
            "reference_median_seconds": _text(verdict.reference_median),
            "slow_minute_median_seconds": _text(verdict.slow_minute_median),
        },
        "master_wait_overlaps": [overlap(o) for o in cut("master_wait_overlaps", links.overlaps, _PAYLOAD_LIST_LIMIT)],
        "minutes": [{"minute": _iso(m.minute), "slow_queries": m.slow_queries, "master_waits": m.master_waits}
                    for m in cut("minutes", links.minutes, _PAYLOAD_MINUTE_LIMIT)],
        "node_cards": [node(c) for c in cut("node_cards", sorted(links.nodes, key=_node_priority),
                                            _PAYLOAD_LIST_LIMIT)],
        "slow_query_profile": [{
            "cmd": p.cmd, "date_range": p.date_range, "index_count": p.index_count, "total": p.total,
            "slow": p.slow, "median_seconds": _text(p.median_seconds), "max_seconds": _text(p.max_seconds),
        } for p in links.profile],
    }
    if omitted:
        payload["omitted"] = omitted
    return payload
