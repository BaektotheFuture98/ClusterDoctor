"""``DiagnosisReport``를 평문으로 그린다.

줄 포맷이 여기 한 벌만 있다. HTML 어댑터는 같은 함수로 만든 줄을 ``<pre>``에
담고, ``StdoutNotifier``와 HTML의 ``<details>`` 원문 블록은 ``render_text``가
만든 문서를 그대로 쓴다. 세 곳이 각자 그리면 같은 관측값이 화면마다 다르게
보이고, 한쪽만 고쳐지는 사고가 난다.

표현이 도메인이 아니라 여기 있는 이유는 ``format_log_line``이 프롬프트 쪽에
있는 것과 같다 — 어떤 값을 어떻게 보여줄지는 표현의 관심사다.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from cluster_doctor.domain.model.diagnosis_report import (
    DiagnosisReport,
    HealthPoint,
    NodeMetricRow,
    Observations,
    SlowCandidate,
    TimelineRow,
)

_KST = timezone(timedelta(hours=9))

# 타임라인에 항상 찍는 소스. counts에 키가 없어도 0으로 그린다.
#
# 빠뜨리면 "측정하지 않았다"와 "0건이다"가 구별되지 않는다. 실측 사고에서
# slowlog가 0건이었는데 그 칸이 비어 있었다면 운영자는 slowlog를 조회하지
# 않은 것으로 읽었을 것이다 — 실제로는 조회했고 정말 없었다.
_SOURCE_ORDER: tuple[tuple[str, str], ...] = (
    ("slowlog", "slowlog"),
    ("es_query_log", "query"),
    ("node_metric", "metric"),
)


def _hm(moment: datetime) -> str:
    return moment.astimezone(_KST).strftime("%H:%M")


def _hms(moment: datetime) -> str:
    return moment.astimezone(_KST).strftime("%H:%M:%S")


def _ymd_hms(moment: datetime) -> str:
    return moment.astimezone(_KST).strftime("%Y-%m-%d %H:%M:%S")


def timeline_line(row: TimelineRow) -> str:
    """분 한 칸. 소스마다 칸이 따로 있는 것이 요점이다.

    예전에는 ``slowlog=<건수>`` 한 칸에 세 소스가 뭉개졌고, 그래서
    es_query_log 264건이 slowlog 건수로 리포트에 실렸다(실측).
    """
    counts = " ".join(
        f"{label}={row.counts.get(key, 0)}" for key, label in _SOURCE_ORDER
    )
    heap = "-"
    if row.jvm_heap_max is not None:
        node = f"({row.jvm_heap_max_node})" if row.jvm_heap_max_node else ""
        heap = f"{row.jvm_heap_max}%{node}"
    line = (
        f"{_hm(row.minute)}  {counts}  "
        f"took_max={row.took_max or '-'} "
        f"runtime_max={f'{row.runtime_max}s' if row.runtime_max is not None else '-'} "
        f"jvm_heap_max={heap} "
        f"rejected={row.search_rejected_max}/{row.write_rejected_max}"
    )
    if row.failed:
        line += "  [분석 실패]"
    return line


def node_line(row: NodeMetricRow) -> str:
    """노드 한 대의 구간 최대값.

    ``os_mem``에 "(캐시포함)"을 붙이는 것은 프롬프트·로그 줄과 같은 이유다.
    ES는 남는 RAM을 파일시스템 캐시로 쓰므로 95~99%가 정상인데, ``mem``이라고만
    적으면 읽는 사람이 메모리 부족으로 읽는다(실제로 그런 리포트가 나왔다).
    """
    return (
        f"{row.node:<12} jvm_heap={row.jvm_heap_max}% cpu={row.cpu_max}% "
        f"os_mem(캐시포함)={row.os_mem_max}% "
        f"search(queue={row.search_queue_max},rejected={row.search_rejected_max}) "
        f"write(queue={row.write_queue_max},rejected={row.write_rejected_max}) "
        f"samples={row.samples}"
    )


# 리포트에 싣는 노드 줄 수. 이 클러스터는 노드가 107대라 전부 실으면 그중
# 대부분이 "정상 범위"인 똑같은 줄이 되고, 정작 눈에 띄어야 할 노드가 그 안에
# 묻힌다. 잘린 사실은 헤더의 "N개 중 M개"로 드러난다.
_NODE_RENDER_MAX = 20


def _node_rank(row: NodeMetricRow) -> tuple:
    """무엇을 먼저 보여줄지. 이름순이 아니라 문제 순이다.

    rejected가 0이 아닌 것을 맨 앞에 둔다 — 프롬프트가 지표 해석에서 *"rejected는
    0이 아닌 것 자체가 유의미하다"* 고 못 박은 값이고, jvm_heap이 아무리 높아도
    rejected가 0이면 정상 범위라고 읽어야 하기 때문이다.
    """
    rejected = row.search_rejected_max + row.write_rejected_max
    return (-(rejected > 0), -rejected, -row.jvm_heap_max, -row.cpu_max, row.node)


def node_lines(rows: tuple[NodeMetricRow, ...]) -> list[str]:
    """노드 섹션의 줄 전체. 정렬·절단·헤더를 함께 만든다."""
    if not rows:
        return []
    ordered = sorted(rows, key=_node_rank)
    shown = ordered[:_NODE_RENDER_MAX]
    lines = [node_line(row) for row in shown]
    if len(ordered) > len(shown):
        lines.insert(
            0,
            f"노드 {len(ordered)}개 중 {len(shown)}개 "
            "(rejected 발생 · jvm_heap 높은 순)",
        )
    return lines


def health_line(point: HealthPoint) -> str:
    """상태가 유지된 구간 하나. 같은 상태가 이어지면 한 줄로 접혀 있다."""
    span = _hms(point.at)
    if point.until != point.at:
        span += f"~{_hms(point.until)}"
    return (
        f"{span}  status={point.status} "
        f"unassigned={point.unassigned_shards} active={point.active_shards} "
        f"nodes={point.number_of_nodes}"
    )


def candidate_line(candidate: SlowCandidate) -> str:
    """느린 요청 후보 한 건을 **한 줄로**. 수치는 전부 코드가 붙인 값이다.

    선정 이유와 쿼리 원문은 여기 넣지 않는다. 평문은 들여쓴 줄로, HTML은
    하위 목록으로 그려야 해서 담는 모양이 다르다 — ``candidate_details``가
    그 두 가지를 항목으로 돌려주고, 각 렌더러가 자기 방식으로 붙인다.
    """
    parts = [f"[{candidate.candidate_id}]", candidate.source, _hms(candidate.timestamp)]
    if candidate.took:
        parts.append(f"took={candidate.took}")
    if candidate.run_time is not None:
        parts.append(f"runtime={candidate.run_time}s")
    if candidate.total_hits:
        parts.append(f"hits={candidate.total_hits}")
    if candidate.total_shards:
        parts.append(f"shards={candidate.total_shards}")
    if candidate.index_name:
        parts.append(f"index={candidate.index_name}")
    if candidate.node:
        parts.append(f"node={candidate.node}")
    if candidate.cmd:
        parts.append(f"cmd={candidate.cmd}")
    if candidate.company:
        parts.append(f"company={candidate.company}")
    if candidate.user:
        parts.append(f"user={candidate.user}")
    return " ".join(parts)


def candidate_details(candidate: SlowCandidate, reason: str = "") -> list[str]:
    """후보 한 건의 하위 항목. 없으면 빈 리스트.

    ``reason``은 모델이 그 후보를 고른 이유다. 고르지 않았으면 비어 있고,
    그때도 후보 줄 자체는 남는다 — 후보였다는 사실이 관측값이기 때문이다.
    """
    details: list[str] = []
    if reason:
        details.append(f"선정 이유: {reason}")
    if candidate.query:
        details.append(f"query: {candidate.query}")
    return details


def overview_lines(obs: Observations) -> list[str]:
    """1번 섹션. 전부 코드가 관측한 값이다.

    관측한 것이 하나도 없으면 빈 리스트를 돌려준다. ``시각 기준: -`` 과
    ``총 대기 시간: 0초``만 적힌 섹션은 정보가 아니라 잡음이고, 그런 섹션이
    목차에 자리를 차지하면 진짜 내용이 밀린다.
    """
    if not (obs.first_seen or obs.requested or obs.time_basis or obs.total_wait_seconds):
        return []

    lines: list[str] = []
    if obs.first_seen and obs.last_seen:
        lines.append(
            f"유입 관찰: {_ymd_hms(obs.first_seen)} ~ {_ymd_hms(obs.last_seen)}"
        )
    elif obs.first_seen:
        lines.append(f"유입 관찰: {_ymd_hms(obs.first_seen)} (종료 시각 미관측)")
    if obs.requested:
        windows = ", ".join(
            f"{_ymd_hms(start)} ~ {_hms(end)}" for start, end in obs.requested
        )
        lines.append(f"분석 구간: {windows}")
    lines.append(f"사용한 시각 기준: {obs.time_basis or '-'}")
    cap = " (대기 상한 도달)" if obs.wait_cap_reached else ""
    lines.append(f"총 대기 시간: {obs.total_wait_seconds:.0f}초{cap}")
    return lines


def _section(title: str, lines: list[str]) -> list[str]:
    if not lines:
        return []
    return [title, "─" * 40, *lines, ""]


def render_text(report: DiagnosisReport) -> str:
    """리포트 전체를 평문 한 장으로.

    ``StdoutNotifier``와 HTML의 원문 블록이 이것을 쓴다. HTML 본문은 같은 줄
    함수들로 따로 조립하지만 내용은 같다.
    """
    obs = report.observations
    out: list[str] = []

    out += _section("1. 인시던트 개요", overview_lines(obs))
    out += _section(
        "2. 분 단위 타임라인 (관측값)",
        [timeline_line(row) for row in obs.timeline],
    )
    out += _section(
        "3. 클러스터 상태 이력 (관측값)",
        [health_line(point) for point in obs.health],
    )
    out += _section("4. 노드별 구간 최대값 (관측값)", node_lines(obs.nodes))

    master = list(obs.master_logs)
    if master:
        header = f"마스터 노드 로그 {len(master)}줄"
        if obs.master_log_total > len(master):
            header += f" (전체 {obs.master_log_total}줄 중)"
        out += _section("5. 마스터 노드 로그 (관측값)", [header, *master])

    picks = {
        pick.candidate_id: pick.reason
        for pick in (report.narrative.suspect_picks if report.narrative else ())
    }
    candidate_lines: list[str] = []
    for candidate in obs.candidates:
        candidate_lines.append(candidate_line(candidate))
        candidate_lines += [
            f"      {detail}"
            for detail in candidate_details(candidate, picks.get(candidate.candidate_id, ""))
        ]
    out += _section("6. 느린 요청 후보 (관측값 + 모델 선정)", candidate_lines)

    narrative = report.narrative
    if narrative is not None:
        if narrative.headline:
            out += _section("7. 결론", [narrative.headline, *narrative.context])
        findings = []
        for finding in narrative.findings:
            findings.append(f"{finding.severity}: {finding.title}")
            findings += [f"    - {item}" for item in finding.evidence]
        out += _section("8. 발견된 문제점", findings)
        cause = []
        if narrative.root_cause:
            cause.append(narrative.root_cause)
        cause += [f"근거: {item}" for item in narrative.supporting]
        cause += [f"반박 근거: {item}" for item in narrative.contradicting]
        cause += [f"확인하지 못한 것: {item}" for item in narrative.unverified]
        out += _section("9. 근본 원인", cause)
        out += _section("10. 권장 조치", list(narrative.recommendations))
    elif report.narrative_text:
        out += _section("7. 모델 리포트 (평문)", report.narrative_text.splitlines())

    return "\n".join(out).rstrip() + "\n"
