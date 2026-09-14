"""로그에서 관측값을 계산한다. 순수 함수만 둔다.

LLM도 tool도 모른다. 덕분에 테스트가 외부 의존 없이 돌고, 분 단위 노드
(``langgraph/nodes.py``)와 구간 단위 tool(``deepagent/tools.py``)이 같은 함수를
쓴다. 두 곳이 각자 세면 같은 값이 두 벌 생기고, 한쪽만 고쳐지는 사고가 난다.

표현은 여기 없다. 프롬프트 줄로 그리는 것은 ``langgraph/prompts.py``의 관심사다
(``format_log_line``이 도메인이 아니라 그쪽에 있는 것과 같은 이유).
"""

from __future__ import annotations

import re
from dataclasses import replace
from datetime import datetime

from cluster_doctor.domain.model.diagnosis_report import (
    NodeMetricRow,
    SlowCandidate,
    TimelineRow,
)
from cluster_doctor.domain.model.log_entry import LogEntry, QueryLogEntry, SlowlogEntry
from cluster_doctor.domain.model.node_metric import NodeMetricEntry

# ES가 slowlog의 took을 내보내는 표기. 실측(packetbeat.slowlog_v2 612건)에서는
# "37.1s"와 "1m" 두 가지만 나왔지만, ES는 아래 단위를 모두 쓸 수 있으므로
# 전부 받는다.
#
# **긴 접미사를 먼저 매칭해야 한다.** "ms"를 "m"으로 읽으면 500밀리초가
# 500분이 되어 최댓값 비교가 통째로 뒤집힌다. 정규식 교대(|)는 왼쪽부터
# 시도하므로 순서가 곧 규칙이다.
_DURATION_UNITS_MS: tuple[tuple[str, float], ...] = (
    ("nanos", 1e-6),
    ("micros", 1e-3),
    ("ms", 1.0),
    ("s", 1_000.0),
    ("m", 60_000.0),
    ("h", 3_600_000.0),
    ("d", 86_400_000.0),
)
_DURATION_RE = re.compile(
    r"^\s*([0-9]+(?:\.[0-9]+)?)\s*(" + "|".join(u for u, _ in _DURATION_UNITS_MS) + r")\s*$"
)


def parse_duration_ms(text: str) -> int | None:
    """``"37.1s"`` → ``37100``. 읽을 수 없으면 ``None``.

    ``None``을 돌려줘도 호출부는 원문을 버리지 않는다. 숫자로 비교할 수 없다는
    것이 값이 없다는 뜻은 아니고, 리포트는 원문을 인용해야 근거가 된다.
    """
    if not text:
        return None
    match = _DURATION_RE.match(str(text))
    if not match:
        return None
    amount = float(match.group(1))
    unit = match.group(2)
    factor = next(f for u, f in _DURATION_UNITS_MS if u == unit)
    return int(amount * factor)


def count_by_source(logs: list[LogEntry]) -> dict[str, int]:
    """소스별 건수. ``nodes.py``도 이 함수를 쓴다 — 세는 곳이 둘이면 언젠가
    서로 다른 값을 말한다."""
    counts: dict[str, int] = {}
    for log in logs:
        counts[log.source] = counts.get(log.source, 0) + 1
    return counts


def timeline_row(minute: datetime, logs: list[LogEntry], *, failed: bool = False) -> TimelineRow:
    """분 한 칸의 관측값을 만든다.

    ``counts``가 소스별로 갈려 있는 것이 요점이다. 예전에는 프롬프트가 이 값을
    ``slowlog=<건수>`` 한 칸으로 뭉개라고 시켰고, 그래서 es_query_log 264건이
    slowlog 건수로 리포트에 실렸다(실측).
    """
    took_max = ""
    took_max_ms: int | None = None
    runtime_max = None
    jvm_heap_max: int | None = None
    jvm_heap_max_node = ""
    search_rejected_max = 0
    write_rejected_max = 0

    for log in logs:
        if isinstance(log, SlowlogEntry):
            ms = parse_duration_ms(log.took)
            # 파싱된 값끼리는 숫자로 비교한다. 하나도 파싱되지 않으면 첫 원문을
            # 남겨 두어 "값이 있었다"는 사실만이라도 리포트에 도달하게 한다.
            if ms is not None and (took_max_ms is None or ms > took_max_ms):
                took_max_ms, took_max = ms, log.took
            elif took_max_ms is None and not took_max and log.took:
                took_max = log.took
        elif isinstance(log, QueryLogEntry):
            if log.run_time is not None and (runtime_max is None or log.run_time > runtime_max):
                runtime_max = log.run_time
        elif isinstance(log, NodeMetricEntry):
            if jvm_heap_max is None or log.jvm_heap_used_percent > jvm_heap_max:
                jvm_heap_max = log.jvm_heap_used_percent
                jvm_heap_max_node = log.node_name
            search_rejected_max = max(search_rejected_max, log.search_rejected)
            write_rejected_max = max(write_rejected_max, log.write_rejected)

    return TimelineRow(
        minute=minute,
        counts=count_by_source(logs),
        took_max=took_max,
        took_max_ms=took_max_ms,
        runtime_max=runtime_max,
        jvm_heap_max=jvm_heap_max,
        jvm_heap_max_node=jvm_heap_max_node,
        search_rejected_max=search_rejected_max,
        write_rejected_max=write_rejected_max,
        failed=failed,
    )


def node_metric_summary(logs: list[LogEntry]) -> dict[str, NodeMetricRow]:
    """노드별 구간 최대값. 키는 노드 이름."""
    rows: dict[str, NodeMetricRow] = {}
    for log in logs:
        if not isinstance(log, NodeMetricEntry):
            continue
        current = rows.get(log.node_name)
        if current is None:
            rows[log.node_name] = NodeMetricRow(
                node=log.node_name,
                samples=1,
                jvm_heap_max=log.jvm_heap_used_percent,
                cpu_max=log.os_cpu_percent,
                os_mem_max=log.os_mem_used_percent,
                search_queue_max=log.search_queue,
                search_rejected_max=log.search_rejected,
                write_queue_max=log.write_queue,
                write_rejected_max=log.write_rejected,
            )
            continue
        rows[log.node_name] = replace(
            current,
            samples=current.samples + 1,
            jvm_heap_max=max(current.jvm_heap_max, log.jvm_heap_used_percent),
            cpu_max=max(current.cpu_max, log.os_cpu_percent),
            os_mem_max=max(current.os_mem_max, log.os_mem_used_percent),
            search_queue_max=max(current.search_queue_max, log.search_queue),
            search_rejected_max=max(current.search_rejected_max, log.search_rejected),
            write_queue_max=max(current.write_queue_max, log.write_queue),
            write_rejected_max=max(current.write_rejected_max, log.write_rejected),
        )
    return rows


def merge_node_rows(acc: dict[str, NodeMetricRow], new: dict[str, NodeMetricRow]) -> None:
    """``acc``에 ``new``를 제자리에서 병합한다. 지표마다 max의 max.

    ``analyze_logs``가 여러 번 불릴 때 쓴다. 리스트에 이어 붙이면 겹친 구간의
    같은 노드가 두 줄로 실린다.
    """
    for node, row in new.items():
        current = acc.get(node)
        if current is None:
            acc[node] = row
            continue
        acc[node] = replace(
            current,
            samples=current.samples + row.samples,
            jvm_heap_max=max(current.jvm_heap_max, row.jvm_heap_max),
            cpu_max=max(current.cpu_max, row.cpu_max),
            os_mem_max=max(current.os_mem_max, row.os_mem_max),
            search_queue_max=max(current.search_queue_max, row.search_queue_max),
            search_rejected_max=max(current.search_rejected_max, row.search_rejected_max),
            write_queue_max=max(current.write_queue_max, row.write_queue_max),
            write_rejected_max=max(current.write_rejected_max, row.write_rejected_max),
        )


def candidate_key(candidate: SlowCandidate) -> tuple:
    """같은 요청인지 판정하는 키. id는 보지 않는다.

    id는 발견 순서대로 붙으므로, 겹친 구간을 다시 조회했을 때 같은 요청에 다른
    id가 붙는 것을 막으려면 내용으로 비교해야 한다.
    """
    return (
        candidate.source,
        candidate.timestamp,
        candidate.node,
        candidate.took,
        candidate.run_time,
    )


def slow_candidates(logs: list[LogEntry], limit: int = 5) -> list[SlowCandidate]:
    """느린 요청 후보를 고른다. ``candidate_id``는 비운 채 돌려준다.

    id를 여기서 붙이지 않는 이유: 여러 번의 ``analyze_logs`` 호출에 걸쳐
    번호가 이어져야 하는데, 그 상태는 호출부(``run_state``)가 갖고 있다.

    두 소스를 각각 상위 ``limit``건씩 고른다. 한쪽으로 합쳐 정렬하면 단위가
    다른 값(``took`` 문자열과 ``run_time`` Decimal)을 견줘야 하고, 실측처럼
    slowlog가 0건인 구간에서는 쿼리 로그 후보마저 밀려날 수 있다.
    """
    slowlogs = [log for log in logs if isinstance(log, SlowlogEntry)]
    queries = [log for log in logs if isinstance(log, QueryLogEntry)]

    picked: list[SlowCandidate] = []

    slowlogs.sort(key=lambda e: (parse_duration_ms(e.took) or -1), reverse=True)
    for entry in slowlogs[:limit]:
        picked.append(
            SlowCandidate(
                candidate_id="",
                source=entry.source,
                timestamp=entry.timestamp,
                index_name=entry.index_name,
                node=entry.node,
                took=entry.took,
                took_ms=parse_duration_ms(entry.took),
                total_hits=entry.total_hits,
                total_shards=entry.total_shards,
                query=entry.query,
            )
        )

    queries.sort(key=lambda e: (e.run_time if e.run_time is not None else -1), reverse=True)
    for entry in queries[:limit]:
        picked.append(
            SlowCandidate(
                candidate_id="",
                source=entry.source,
                timestamp=entry.timestamp,
                node=entry.host,
                run_time=entry.run_time,
                cmd=entry.cmd,
                company=entry.company or "",
                user=entry.user or "",
            )
        )

    return picked
