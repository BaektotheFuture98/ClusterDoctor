"""코드가 수집한 진단 관측값과 그 병합·심각도 규칙."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal
from typing import Literal

from cluster_doctor.incident_analysis_agent.model.health_point import HealthPoint
from cluster_doctor.incident_analysis_agent.model.log_entries import (
    QueryLogEntry,
    record_json,
)


@dataclass(frozen=True)
class TimelineRow:
    """분 한 칸의 관측값. 전부 코드가 센 값이다.

    ``counts``를 ``dict``로 통째로 싣는 것이 위 결함 1의 직접적인 수정이다.
    소스마다 칸이 생기므로 ``slowlog=0 es_query_log=264``처럼 나란히 적힌다.
    렌더러가 그리는 것이지 모델이 옮겨 적는 것이 아니다.

    ``took_max``는 원문 표기(``"37.1s"``)를 그대로 들고 있고 ``took_max_ms``가
    비교용 숫자다. 파싱에 실패해도 원문은 버리지 않는다 — 숫자로 비교할 수
    없다는 것이 값이 없다는 뜻은 아니다.
    """

    minute: datetime
    counts: dict[str, int] = field(default_factory=dict)
    took_max: str = ""
    took_max_ms: int | None = None
    runtime_max: Decimal | None = None
    jvm_heap_max: int | None = None
    jvm_heap_max_node: str = ""
    search_rejected_max: int = 0
    write_rejected_max: int = 0
    # 그 분의 LLM 분석이 실패했는가. 빈칸으로 두면 "아무 일도 없던 분"으로
    # 읽히므로 건수는 채우되 실패 사실을 따로 표시한다.
    failed: bool = False


@dataclass(frozen=True)
class NodeMetricRow:
    """한 노드의 구간 최대값.

    ``search_rejected``·``write_rejected``는 ``_nodes/stats``의 **누적**
    카운터다. 최댓값은 구간 증가분이 아니며 재시작 시 감소할 수 있다.
    현재 구간의 실패 건수나 심각도를 이 값만으로 판정하지 않는다.
    """

    node: str
    samples: int = 0
    jvm_heap_max: int = 0
    cpu_max: int = 0
    os_mem_max: int = 0
    search_queue_max: int = 0
    search_rejected_max: int = 0
    write_queue_max: int = 0
    write_rejected_max: int = 0


@dataclass(frozen=True)
class MasterEvent:
    """마스터 노드 로그 한 줄.

    렌더된 문자열이 아니라 구조로 들고 있는 이유: 리포트에 전부 싣지 않고
    **같은 사건끼리 묶어야** 하기 때문이다. 실측에서 24줄 중 20줄이 같은
    ``follower_check`` 타임아웃이었고 대상 노드 이름만 달랐다. 한 줄이 524자라
    그대로 실으면 리포트의 72%를 그 20줄이 차지한다.

    묶으려면 ``logger``와 ``action``이 값으로 있어야 한다. 문자열에서 매번
    정규식으로 뽑으면 렌더 형식이 바뀔 때 조용히 묶이지 않는다.
    """

    timestamp: datetime | None
    node: str = ""
    level: str = ""
    logger: str = ""
    line: str = ""
    # 렌더된 한 줄. 대표 줄을 원문 그대로 인용할 때 쓴다 — 근거는 원문이어야
    # 근거다.
    rendered: str = ""


@dataclass(frozen=True)
class SlowCandidate:
    """느린 요청 후보 한 건. 코드가 골라 id를 붙인다.

    모델에게는 이 목록을 ``[C1] [C2] …`` 형태로 보여주고 **id와 이유만**
    돌려받는다. 수치와 쿼리 원문은 코드가 여기서 붙이므로, 위 결함 2의
    ``미확인``과 전사 오류가 구조적으로 불가능해진다.

    ``source``로 두 출처를 구분한다. slowlog에만 ``took``·``total_hits``가
    있고 쿼리 로그에는 ``run_time``·``cmd``가 있다. 한쪽에 없는 칸을 모델이
    지어내지 않도록 비어 있는 채로 둔다.
    """

    candidate_id: str
    source: str
    timestamp: datetime
    index_name: str = ""
    node: str = ""
    took: str = ""
    took_ms: int | None = None
    total_hits: str = ""
    total_shards: int = 0
    run_time: Decimal | None = None
    cmd: str = ""
    company: str = ""
    user: str = ""
    query: str = ""
    request_host: str = ""
    target_host: str = ""
    query_record_key: str = ""


@dataclass(frozen=True)
class SourceWindowStatus:
    source: str
    start: datetime
    end: datetime
    status: Literal["ok", "failed", "limited", "skipped"]
    row_count: int | None
    collected_at: datetime
    error: str = ""
    host: str = ""


@dataclass(frozen=True)
class Observations:
    """코드가 관측한 사실 전부. 모델을 거치지 않는다.

    한 Incident 안에서 분석이 여러 번 일어날 수 있으므로 값들이 누적된다.
    누적 규칙은 수집하는 쪽(``service/observation/builder.py``와
    ``merge_observations``)에 있고, 여기 도착할 때는 이미 병합이 끝나 있다.
    """

    time_basis: str = ""
    first_seen: datetime | None = None
    last_seen: datetime | None = None
    # 실제로 분석을 요청한 구간들. 중복을 허용한다 — 같은 구간을 다시
    # 부른 것은 그 자체로 사실이다.
    requested: tuple[tuple[datetime, datetime], ...] = ()
    total_wait_seconds: float = 0.0
    wait_cap_reached: bool = False
    timeline: tuple[TimelineRow, ...] = ()
    nodes: tuple[NodeMetricRow, ...] = ()
    # 마스터 노드 로그. 상한에 걸렸는지 보이려고 전체 건수를 따로 든다.
    master_events: tuple[MasterEvent, ...] = ()
    master_log_total: int = 0
    health: tuple[HealthPoint, ...] = ()
    candidates: tuple[SlowCandidate, ...] = ()
    query_requests: tuple[QueryLogEntry, ...] = ()
    source_statuses: tuple[SourceWindowStatus, ...] = ()


def observed_severity(obs: Observations) -> tuple[str, tuple[str, ...]]:
    """Severity from observed events, never lifetime rejection counters."""
    reasons: list[str] = []
    level = ""

    errors = sum(1 for e in obs.master_events if e.level.upper() == "ERROR")
    warns = sum(1 for e in obs.master_events if e.level.upper() == "WARN")
    failed_minutes = sum(1 for row in obs.timeline if row.failed)

    if errors:
        level = level or "Warning"
        reasons.append(f"마스터 로그 ERROR {errors}건")
    if failed_minutes:
        level = level or "Warning"
        reasons.append(f"분석하지 못한 분 {failed_minutes}개")
    if warns:
        level = level or "Info"
        reasons.append(f"마스터 로그 WARN {warns}건")

    for point in obs.health:
        if point.status and point.status.lower() != "green" and _inside(point, obs):
            level = (
                "Critical" if point.status.lower() == "red" else (level or "Warning")
            )
            reasons.append(f"클러스터 상태 {point.status}")
            break

    return level, tuple(reasons)


def _inside(point: HealthPoint, obs: Observations) -> bool:
    """상태 관측이 분석 구간 안에서 일어났는가."""
    if not obs.requested:
        return False
    start = min(s for s, _e in obs.requested)
    end = max(e for _s, e in obs.requested)
    return start <= point.at <= end


def merge_node_row(current: NodeMetricRow, new: NodeMetricRow) -> NodeMetricRow:
    """같은 노드의 두 관측을 합친다. 지표마다 max의 max, 표본은 합.

    쌍 단위 병합을 도메인에 두는 이유: 이 값을 합치는 곳이 둘이다. 한 번의
    분석 안에서 구간을 합칠 때(``observations.merge_node_rows``)와, 한 Incident
    안에서 여러 번의 분석을 합칠 때(``merge_observations``, ``MainAgentState.observations``에
    접힌다). 규칙이 두 벌이 되면 한쪽만 고쳐지는 날이 온다.
    """
    return replace(
        current,
        samples=current.samples + new.samples,
        jvm_heap_max=max(current.jvm_heap_max, new.jvm_heap_max),
        cpu_max=max(current.cpu_max, new.cpu_max),
        os_mem_max=max(current.os_mem_max, new.os_mem_max),
        search_queue_max=max(current.search_queue_max, new.search_queue_max),
        search_rejected_max=max(current.search_rejected_max, new.search_rejected_max),
        write_queue_max=max(current.write_queue_max, new.write_queue_max),
        write_rejected_max=max(current.write_rejected_max, new.write_rejected_max),
    )


def merge_observations(current: Observations, new: Observations) -> Observations:
    """한 Incident 안에서 여러 번의 분석 결과를 하나로 합친다.

    누적 규칙이 값마다 다른 것이 요점이다.

      timeline        분을 키로 덮어쓴다. 실패한 분을 다시 분석해 성공하면
                      failed 표시가 사라져야 한다.
      nodes           노드를 키로 지표별 max.
      master_events   (시각, 노드, 줄)로 중복 제거. 겹친 구간을 다시 조회하면
                      같은 줄이 두 번 온다.
      candidates      ``candidate_id``로 중복 제거. id는 한 번 붙으면 바뀌지
                      않으므로 그 자체가 키다.
      health          시간순 이력이라 이어 붙인다.
      first/last_seen 바깥쪽으로 넓힌다.
    """
    timeline = {row.minute: row for row in current.timeline}
    timeline.update({row.minute: row for row in new.timeline})

    nodes = {row.node: row for row in current.nodes}
    for row in new.nodes:
        existing = nodes.get(row.node)
        nodes[row.node] = row if existing is None else merge_node_row(existing, row)

    events: dict[tuple, MasterEvent] = {
        (event.timestamp, event.node, event.line): event
        for event in current.master_events + new.master_events
    }

    candidates = {item.candidate_id: item for item in current.candidates}
    candidates.update({item.candidate_id: item for item in new.candidates})

    return Observations(
        time_basis=current.time_basis or new.time_basis,
        first_seen=_earlier(current.first_seen, new.first_seen),
        last_seen=_later(current.last_seen, new.last_seen),
        requested=current.requested + new.requested,
        total_wait_seconds=current.total_wait_seconds + new.total_wait_seconds,
        wait_cap_reached=current.wait_cap_reached or new.wait_cap_reached,
        timeline=tuple(timeline[minute] for minute in sorted(timeline)),
        nodes=tuple(sorted(nodes.values(), key=lambda row: row.node)),
        master_events=tuple(events.values()),
        master_log_total=current.master_log_total + new.master_log_total,
        health=current.health + new.health,
        query_requests=merge_query_requests(current.query_requests, new.query_requests),
        source_statuses=tuple(dict.fromkeys(current.source_statuses + new.source_statuses)),
        candidates=tuple(
            sorted(
                candidates.values(), key=lambda c: (len(c.candidate_id), c.candidate_id)
            )
        ),
    )


def _earlier(left: datetime | None, right: datetime | None) -> datetime | None:
    if left is None:
        return right
    if right is None:
        return left
    return min(left, right)


def _later(left: datetime | None, right: datetime | None) -> datetime | None:
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


def merge_query_requests(
    current: tuple[QueryLogEntry, ...], new: tuple[QueryLogEntry, ...]
) -> tuple[QueryLogEntry, ...]:
    """Keep batch multiplicity while avoiding double counting overlapping fetches.

    There is no request ID in the fetched schema. Identical full records use the
    maximum observed multiplicity; collection timestamps are not part of identity.
    """
    existing = Counter(record_json(item) for item in current)
    incoming = Counter()
    result = list(current)
    for item in new:
        key = record_json(item)
        incoming[key] += 1
        if incoming[key] > existing[key]:
            result.append(item)
    return tuple(sorted(result, key=lambda item: item.timestamp))
