"""소스 사이의 시간·노드 연결. 모델이 아니라 코드가 대조한다.

쿼리 로그 시각(``reg_date``)이 요청 시작인지 종료인지 확정되지 않았다. 그래서
겹침은 두 가정으로 각각 계산하고, 두 가정 모두에서 겹친 것과 한 가정에서만
겹친 것을 나눠 둔다. 한쪽 가정의 결과를 사실처럼 싣지 않기 위해서다.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from statistics import median
from typing import Literal

from cluster_doctor.incident_analysis_agent.datasource.clickhouse.node_metric import DEFAULT_THRESHOLDS, NodeMetricThresholds
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
# 같은 형태의 기준 미만 요청 중앙값이 기준 분보다 이 배수를 넘게 늘면 그 형태가 함께 느려진 것으로 본다.
_SPREAD_RATIO = Decimal("2")
_MIN_SAMPLES = 20
_CLUSTER_WIDE_MIN_SHAPES = 2
# 기준 중앙값이 0에 가까우면 아주 작은 변화도 배수로는 커진다. 바닥값을 둔다.
_MEDIAN_FLOOR = Decimal("0.05")
# 집중도 요약의 상한. 형태 3개 x (요청자 3, 호스트 3)로 컨텍스트가 일정한 크기를 넘지 않는다.
_CONCENTRATION_SHAPES = 3
_CONCENTRATION_TOP = 3


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
class ShapeConcentration:
    """느린 요청이 많은 형태에서 누가·어디서·몇 개 키워드로 보냈는가.

    프로파일은 형태별 수치만 있어 느린 요청이 한 요청자에 몰렸는지 알 수 없다. 모델이 원본 행에서 다시 세지
    않도록 코드가 상위 몇 개만 센다. requesters/hosts의 total은 같은 형태의 전체 요청 수다.
    """

    shape: tuple[str, int, int]
    slow: int
    requesters: tuple[tuple[str, str, int, int], ...]
    request_hosts: tuple[tuple[str, int, int], ...]
    # 저장된 keyword는 최대 5개라 버려진 개수를 더해 센다. 기준 미만 요청의 최대값과 나란히 비교하기 위한 값이다.
    keyword_count_max_slow: int
    keyword_count_max_other: int | None


@dataclass(frozen=True)
class Verdict:
    """지연이 특정 형태의 요청에 한정됐는가, 같은 형태의 다른 요청도 느려졌는가.

    기준은 과거가 아니라 같은 분석 구간 안의 "느린 요청이 없던 분"이다. 형태마다 기준 미만 요청만 두 쪽으로 나눠 비교하고,
    reference_median / slow_minute_median은 비교 가능한 형태들의 그 요청을 합친 중앙값이다.
    """

    scope: Literal["specific", "cluster_wide", "undetermined", "no_slow", "no_query_log"]
    slow_shapes: tuple[tuple[str, int, int], ...]
    reference_median: Decimal | None
    slow_minute_median: Decimal | None
    # 두 쪽 모두 표본이 충분해 비교한 형태 수와, 그중 느린 분의 중앙값이 늘어난 형태.
    compared_shapes: int = 0
    widened_shapes: tuple[tuple[str, int, int], ...] = ()
    # slow_shapes와 같은 순서의 형태별 느린 요청 수.
    slow_counts: tuple[int, ...] = ()
    # no_query_log일 때만 쓴다. 수집 실패와 "수집했지만 비어 있음"을 문장에서 구분하기 위함이다.
    query_log_failed: bool = False


@dataclass(frozen=True)
class CrossLinks:
    slow_threshold: Decimal
    overlaps: tuple[WaitOverlap, ...]
    minutes: tuple[MinuteLink, ...]
    nodes: tuple[NodeCard, ...]
    profile: tuple[QueryProfileRow, ...]
    verdict: Verdict
    # 프로파일은 상한으로 잘리므로 자르기 전 형태 수를 남겨 잘린 건수를 알린다.
    profile_total: int = 0
    concentration: tuple[ShapeConcentration, ...] = ()


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
        if wait.sent_at is None or wait.received_at is None:
            continue
        # 겹침 표와 같은 기준이 되도록 대기 구간이 걸치는 모든 분에 센다.
        minute = _minute(wait.sent_at)
        while minute <= wait.received_at:
            waits_by_minute[minute] += 1
            minute += timedelta(minutes=1)
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
    return tuple(rows)


def _concentration(observations: Observations, slow_seconds: Decimal) -> tuple[ShapeConcentration, ...]:
    by_shape: dict[tuple[str, int, int], list[tuple[QueryLogEntry, Decimal | None]]] = defaultdict(list)
    for r in observations.query_requests:
        by_shape[query_shape(r)].append((r, valid_runtime(r.run_time)))

    def is_slow(v: Decimal | None) -> bool:
        return v is not None and v >= slow_seconds

    def top(counter_slow: Counter, counter_all: Counter) -> list:
        # 느린 요청이 많은 순, 같으면 전체 요청이 적은 쪽(더 집중된 쪽)이 앞에 온다.
        keys = sorted(counter_slow, key=lambda k: (-counter_slow[k], counter_all[k], k))
        return [k for k in keys[:_CONCENTRATION_TOP]]

    rows = []
    shapes = [s for s, items in by_shape.items() if any(is_slow(v) for _, v in items)]
    shapes.sort(key=lambda s: (-sum(1 for _, v in by_shape[s] if is_slow(v)),
                               -max(v for _, v in by_shape[s] if is_slow(v)), s))
    for shape in shapes[:_CONCENTRATION_SHAPES]:
        items = by_shape[shape]
        slow = [r for r, v in items if is_slow(v)]
        other = [r for r, v in items if not is_slow(v)]
        who_all = Counter((r.company, r.user) for r, _ in items)
        who_slow = Counter((r.company, r.user) for r in slow)
        host_all = Counter(r.host for r, _ in items)
        host_slow = Counter(r.host for r in slow)
        count = lambda r: len(r.keyword) + r.keyword_omitted
        rows.append(ShapeConcentration(
            shape=shape, slow=len(slow),
            requesters=tuple((c, u, who_slow[(c, u)], who_all[(c, u)]) for c, u in top(who_slow, who_all)),
            request_hosts=tuple((h, host_slow[h], host_all[h]) for h in top(host_slow, host_all)),
            keyword_count_max_slow=max(count(r) for r in slow),
            keyword_count_max_other=max((count(r) for r in other), default=None),
        ))
    return tuple(rows)


def concrete_targets(observations: Observations) -> frozenset[str]:
    """조치 문장이 가리킬 수 있는 구체 대상 문자열. 형태 라벨은 날짜 범위·인덱스 수 표기로 포함한다."""
    names: set[str] = set()
    for r in observations.query_requests:
        names.update((r.company, r.user, _host(r.host), _host(r.target_host)))
        names.add(f"{r.date_range}일")
        names.add(f"인덱스 {query_shape(r)[2]}개")
    for node in observations.nodes:
        names.update((node.node, node.node_ip))
    for status in observations.source_statuses:
        names.add(status.host)
    for card in _node_cards(observations, parse_master_waits(observations.master_events)):
        names.update((card.node, card.node_ip))
    # 짧은 값은 어떤 문장에도 우연히 들어가 검사를 무력화하므로 쓰지 않는다.
    return frozenset(n for n in names if n and len(n) >= 3)


def judge_spread(observations: Observations, *, slow_seconds: Decimal = SLOW_QUERY_SECONDS) -> Verdict:
    if not observations.query_requests:
        # 로그가 없는 것은 느린 요청이 없다는 뜻이 아니다. 수집 실패 여부만 따로 남긴다.
        failed = any(s.source == "es_query_log" and s.status == "failed" for s in observations.source_statuses)
        return Verdict("no_query_log", (), None, None, query_log_failed=failed)
    measured = [(r, v) for r in observations.query_requests if (v := valid_runtime(r.run_time)) is not None]
    slow_by_shape: dict[tuple[str, int, int], list[Decimal]] = defaultdict(list)
    for r, v in measured:
        if v >= slow_seconds:
            slow_by_shape[query_shape(r)].append(v)
    total_by_shape = Counter(query_shape(r) for r in observations.query_requests)
    # 문장에는 앞쪽 몇 개만 실리므로 사전순이 아니라 중요한 형태(느린 요청이 많은 순)가 앞에 오게 한다. _profile과 같은 기준이다.
    slow_shapes = tuple(sorted(
        slow_by_shape,
        key=lambda s: (-len(slow_by_shape[s]), -max(slow_by_shape[s]), -total_by_shape[s], s),
    ))
    if not slow_shapes:
        return Verdict("no_slow", (), None, None)
    slow_counts = tuple(len(slow_by_shape[s]) for s in slow_shapes)
    slow_minutes = {_minute(r.timestamp) for r, v in measured if v >= slow_seconds}
    # 느린 요청이 있는 형태를 비교에서 빼면 한 번이라도 느렸던 형태가 모두 사라지고, 남는 것은 애초에 빠른 형태뿐이라
    # 결과가 "특정 형태"로 치우친다. 그래서 형태마다 기준 미만 요청만 골라(느린 요청 자체가 중앙값을 끌어올리지 않게)
    # 같은 형태 안에서 느린 분과 그렇지 않은 분을 비교한다.
    during_by_shape: dict[tuple[str, int, int], list[Decimal]] = defaultdict(list)
    reference_by_shape: dict[tuple[str, int, int], list[Decimal]] = defaultdict(list)
    for r, v in measured:
        if v >= slow_seconds:
            continue
        side = during_by_shape if _minute(r.timestamp) in slow_minutes else reference_by_shape
        side[query_shape(r)].append(v)
    comparable = [s for s in during_by_shape
                  if len(during_by_shape[s]) >= _MIN_SAMPLES and len(reference_by_shape[s]) >= _MIN_SAMPLES]
    if not comparable:
        return Verdict("undetermined", slow_shapes, None, None, slow_counts=slow_counts)
    widened = [s for s in comparable
               if _median(during_by_shape[s]) > max(_median(reference_by_shape[s]), _MEDIAN_FLOOR) * _SPREAD_RATIO]
    reference_median = _median([v for s in comparable for v in reference_by_shape[s]])
    during_median = _median([v for s in comparable for v in during_by_shape[s]])
    # 형태 수의 과반으로 정한다. 요청 수로 가중하면 트래픽이 큰 한 형태가 판정을 좌우해 "여러 형태가 함께 느려졌는가"를
    # 묻는 취지가 흐려지고, 일부 형태만 느려진 경우(특정 형태 문제)를 전반 지연으로 오판할 수 있다.
    # 한 형태의 기준 미만 요청만 느려진 것은 그 형태의 문제이지 여러 요청이 함께 느려진 것이 아니므로 최소 형태 수를 둔다.
    wide = len(widened) >= _CLUSTER_WIDE_MIN_SHAPES and len(widened) * 2 > len(comparable)
    scope = "cluster_wide" if wide else "specific"
    return Verdict(scope, slow_shapes, reference_median, during_median,
                   compared_shapes=len(comparable), widened_shapes=tuple(widened), slow_counts=slow_counts)


def build_cross_links(observations: Observations, *, slow_seconds: Decimal = SLOW_QUERY_SECONDS) -> CrossLinks:
    waits = parse_master_waits(observations.master_events)
    slow = _slow_intervals(observations, slow_seconds)
    profile = _profile(observations, slow_seconds)
    return CrossLinks(
        slow_threshold=slow_seconds,
        overlaps=_wait_overlaps(waits, slow),
        minutes=_minute_links(observations, waits, slow),
        nodes=_node_cards(observations, waits),
        profile=profile[:_PROFILE_ROWS],
        verdict=judge_spread(observations, slow_seconds=slow_seconds),
        profile_total=len(profile),
        concentration=_concentration(observations, slow_seconds),
    )


_PAYLOAD_LIST_LIMIT = 10
# 한 번의 분석은 최대 60분이므로 분 단위 목록의 상한도 같다.
_PAYLOAD_MINUTE_LIMIT = 60


def _iso(moment: datetime | None) -> str | None:
    return moment.astimezone(KST).isoformat() if moment else None


def _text(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def _overlap_priority(overlap: WaitOverlap) -> tuple:
    # 상한에 걸려도 두 가정 모두에서 겹친 대기가 먼저 남도록, 시각순이 아니라 겹침의 확실성 순으로 정렬한다.
    return (not overlap.both, -len(overlap.both), -len(overlap.one_assumption), overlap.wait.sent_at)


def _node_priority(card: NodeCard) -> tuple:
    # 상한에 걸려도 실제 늦은 응답(sent_at이 있는 대기)이 있는 노드가 먼저 남도록 정렬한다.
    return (not any(w.sent_at for w in card.waits), -len(card.waits), card.node)


def cross_links_payload(links: CrossLinks, evidence_ids: dict[str, str],
                        thresholds: NodeMetricThresholds = DEFAULT_THRESHOLDS) -> dict:
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
            "late_responses": sum(1 for w in c.waits if w.sent_at),
            "metrics_max": ({"cpu": c.metrics.cpu_max, "jvm_heap": c.metrics.jvm_heap_max,
                             "search_queue": c.metrics.search_queue_max} if c.metrics else None),
            "node_log": [{"status": s.status, "row_count": s.row_count, "error": s.error} for s in c.node_log],
            "queries_targeting_node": c.queries_targeting,
        }

    verdict = links.verdict
    # 겹치는 쿼리가 없는 대기는 정보가 없어 목록에서 빼고 건수만 남긴다.
    with_queries = sorted((o for o in links.overlaps if o.both or o.one_assumption), key=_overlap_priority)
    without_queries = len(links.overlaps) - len(with_queries)
    if without_queries:
        omitted["master_wait_overlaps_without_queries"] = without_queries
    profile_cut = links.profile_total - len(links.profile)
    if profile_cut > 0:
        omitted["slow_query_profile"] = profile_cut
    payload = {
        "query_time_basis": "쿼리 로그 reg_date가 시작/종료 중 무엇인지 확정되지 않아 두 가정을 모두 계산했다",
        "slow_threshold_seconds": str(links.slow_threshold),
        # 기준을 넘은 노드만 Evidence가 되므로, 넘지 않은 구간에서도 모델이 기준을 알도록 항상 싣는다.
        "node_metric_thresholds": {"heap_warn_percent": thresholds.heap_warn_percent,
                                   "queue_warn": thresholds.queue_warn},
        "verdict": {
            "scope": verdict.scope,
            "slow_shapes": [{"cmd": c, "date_range": d, "index_count": n} for c, d, n in verdict.slow_shapes],
            "reference_median_seconds": _text(verdict.reference_median),
            "slow_minute_median_seconds": _text(verdict.slow_minute_median),
            "compared_shapes": verdict.compared_shapes,
            "widened_shape_count": len(verdict.widened_shapes),
            "widened_shapes": [{"cmd": c, "date_range": d, "index_count": n} for c, d, n in verdict.widened_shapes],
        },
        "master_wait_overlaps": [overlap(o) for o in cut("master_wait_overlaps", with_queries, _PAYLOAD_LIST_LIMIT)],
        "minutes": [{"minute": _iso(m.minute), "slow_queries": m.slow_queries, "master_waits": m.master_waits}
                    for m in cut("minutes", links.minutes, _PAYLOAD_MINUTE_LIMIT)],
        "node_cards": [node(c) for c in cut("node_cards", sorted(links.nodes, key=_node_priority),
                                            _PAYLOAD_LIST_LIMIT)],
        "slow_query_profile": [{
            "cmd": p.cmd, "date_range": p.date_range, "index_count": p.index_count, "total": p.total,
            "slow": p.slow, "median_seconds": _text(p.median_seconds), "max_seconds": _text(p.max_seconds),
        } for p in links.profile],
        "slow_concentration": [{
            "cmd": c.shape[0], "date_range": c.shape[1], "index_count": c.shape[2], "slow": c.slow,
            "requesters": [{"company": co, "user": u, "slow": s, "total_in_shape": t} for co, u, s, t in c.requesters],
            "request_hosts": [{"host": h, "slow": s, "total_in_shape": t} for h, s, t in c.request_hosts],
            "keyword_count_max_slow": c.keyword_count_max_slow,
            "keyword_count_max_not_slow": c.keyword_count_max_other,
        } for c in links.concentration],
    }
    if omitted:
        payload["omitted"] = omitted
    return payload
