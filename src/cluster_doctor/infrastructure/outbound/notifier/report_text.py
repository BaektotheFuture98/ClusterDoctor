"""``DiagnosisReport``를 평문으로 그린다.

줄 포맷이 여기 한 벌만 있다. HTML 어댑터는 같은 함수로 만든 줄을 ``<pre>``에
담고, 로그 폴백과 HTML의 ``<details>`` 원문 블록은 ``render_text``가
만든 문서를 그대로 쓴다. 세 곳이 각자 그리면 같은 관측값이 화면마다 다르게
보이고, 한쪽만 고쳐지는 사고가 난다.

표현이 도메인이 아니라 여기 있는 이유는 ``format_log_line``이 프롬프트 쪽에
있는 것과 같다 — 어떤 값을 어떻게 보여줄지는 표현의 관심사다.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import re
from collections import Counter

from cluster_doctor.domain.model.diagnosis_report import (
    DiagnosisReport,
    HealthPoint,
    MasterEvent,
    NodeMetricRow,
    Observations,
    SlowCandidate,
    observed_severity,
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

    ``slowlog=<건수>`` 한 칸에 세 소스를 뭉치면 실측으로 es_query_log 264건이
    slowlog 건수로 리포트에 실린다.
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
    """노드 섹션의 줄 전체.

    **전 구간 정상이면 목록을 싣지 않는다.** 실측에서 노드 107대 중 rejected가
    0이 아닌 것이 하나도 없었는데, 그때 20줄을 실으면 전부 같은 말을 하는 줄이
    리포트의 13%를 차지한다. 프롬프트의 지표 해석이 "rejected는 0이 아닌 것
    자체가 유의미하다"고 한 기준을 그대로 쓴다 — 유의미한 것이 없으면 그 사실을
    한 줄로 말하고, 대신 최댓값은 남겨 근거로 쓸 수 있게 한다.
    """
    if not rows:
        return []

    flagged = [
        row
        for row in rows
        if row.search_rejected_max or row.write_rejected_max
    ]
    if flagged:
        ordered = sorted(flagged, key=_node_rank)
        shown = ordered[:_NODE_RENDER_MAX]
        lines = [f"rejected가 발생한 노드 {len(ordered)}대 (전체 {len(rows)}대)"]
        lines += [node_line(row) for row in shown]
        if len(ordered) > len(shown):
            lines.append(f"… 외 {len(ordered) - len(shown)}대")
        return lines

    top = max(rows, key=lambda row: row.jvm_heap_max)
    cpu = max(rows, key=lambda row: row.cpu_max)
    return [
        f"노드 {len(rows)}대 전 구간 정상 범위 — search/write rejected 0",
        f"jvm_heap 최대 {top.jvm_heap_max}% ({top.node})",
        f"cpu 최대 {cpu.cpu_max}% ({cpu.node})",
    ]


# 마스터 로그 한 묶음에서 원문으로 인용할 대표 줄 수. 근거는 원문이어야
# 근거이므로 0으로 두지 않고, 같은 말을 반복하지 않도록 1로 둔다.
_MASTER_SAMPLE_PER_GROUP = 1

# 묶음 헤더에 나열할 대상 노드 수. 전부 적으면 헤더가 다시 길어진다.
_MASTER_TARGETS_SHOWN = 6

# ES가 타임아웃 로그에 남기는 대상 action. 같은 action이면 같은 사건으로 본다.
#
# ``[^\]]+``로 잡으면 안 된다. action 이름 자체에 대괄호가 들어간다
# (``cluster:monitor/nodes/stats[n]``) — 첫 ``]``에서 멈추면 ``stats[n``으로
# 잘려 괄호가 깨지고, 서로 다른 두 action이 같은 이름으로 보인다(실측).
# ES 로그 형식이 ``action [...], node [...]``이므로 뒤의 쉼표를 앵커로 쓴다.
# action도 logger도 없는 줄이 모이는 자리. 묶음이 아니라 "묶지 못했다"는
# 표시라, 대표 한 줄로 줄이지 않는다.
_UNGROUPED = "\x00ungrouped"
_MASTER_UNGROUPED_SHOWN = 10

_ACTION_RE = re.compile(r"action \[(.+?)\],")
# 대상 노드는 "node [{RC17-08}{...}]" 형태로 찍힌다.
_TARGET_RE = re.compile(r"node \[\{([^}]+)\}")


def _event_kind(event: MasterEvent) -> str:
    """같은 사건인지 판정하는 키.

    action이 있으면 그것으로 묶는다 — 실측에서 24줄 중 20줄이
    ``internal:coordination/fault_detection/follower_check`` 하나였고, 대상
    노드와 시각만 달랐다. action이 없으면 logger로 묶는다.

    둘 다 없는 줄은 ``_UNGROUPED``로 모은다. 묶은 것이 아니라 **묶지 못한
    것**이라, 렌더러가 이 묶음만 다르게 다룬다(아래).
    """
    match = _ACTION_RE.search(event.line)
    if match:
        return match.group(1)
    return event.logger or _UNGROUPED


def _short_kind(kind: str) -> str:
    """긴 action 이름을 읽을 수 있게 줄인다.

    마지막 마디 하나만 남기면 서로 다른 action이 같은 이름이 된다 —
    ``cluster:monitor/nodes/stats[n]``과 ``indices:monitor/stats[n]``이 둘 다
    ``stats[n]``이 되어 헤더가 두 번 나온다(실측). 두 마디를 남기면 구별된다.
    """
    parts = kind.split("/")
    return "/".join(parts[-2:]) if len(parts) > 1 else kind


def master_log_lines(events: tuple[MasterEvent, ...], total: int = 0) -> list[str]:
    """마스터 로그를 사건별로 묶어 그린다.

    전부 싣지 않는 이유는 부피다. 실측에서 24줄이 리포트의 72%(12,685자)를
    차지했고, 한 줄이 평균 524자였다. 그런데 그중 20줄은 같은 사건이라
    **묶으면 더 잘 읽힌다** — "40초 동안 19대에 대해 20건"이 한눈에 들어온다.

    대표 줄은 원문 그대로 남긴다. 근거로 인용하려면 원문이어야 한다.

    묶지 못한 줄(``_UNGROUPED``)은 예외다. 그 묶음의 구성원은 서로 다른
    사건이므로 대표 한 줄로 줄이면 나머지가 통째로 사라진다 — SSH 폴백으로
    받은 로그가 312줄에서 1줄로 붕괴한 것이 그 경우였다.
    """
    if not events:
        return []

    grouped: dict[str, list[MasterEvent]] = {}
    for event in events:
        grouped.setdefault(_event_kind(event), []).append(event)

    header = f"마스터 노드 로그 {len(events)}건"
    if total > len(events):
        header += f" (전체 {total}건 중)"
    header += f", 사건 {len(grouped)}종"
    lines = [header, ""]

    for kind, items in sorted(grouped.items(), key=lambda kv: -len(kv[1])):
        stamps = [e.timestamp for e in items if e.timestamp]
        span = ""
        if stamps:
            span = f"  {_hms(min(stamps))} ~ {_hms(max(stamps))}"
        label = "묶이지 않은 줄" if kind == _UNGROUPED else _short_kind(kind)
        lines.append(f"[{label}] {len(items)}건{span}")

        targets = Counter(
            match.group(1)
            for match in (_TARGET_RE.search(e.line) for e in items)
            if match
        )
        if targets:
            names = [name for name, _ in targets.most_common(_MASTER_TARGETS_SHOWN)]
            more = f" 외 {len(targets) - len(names)}대" if len(targets) > len(names) else ""
            lines.append(f"  대상 노드 {len(targets)}대: {', '.join(names)}{more}")

        shown = (
            _MASTER_UNGROUPED_SHOWN if kind == _UNGROUPED else _MASTER_SAMPLE_PER_GROUP
        )
        for event in items[:shown]:
            lines.append(f"  {event.rendered}")
        if len(items) > shown:
            lines.append(f"  … 외 {len(items) - shown}건")
        lines.append("")

    return [line for line in lines[:-1]]


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


def health_lines(
    points: tuple[HealthPoint, ...],
    requested: tuple[tuple[datetime, datetime], ...] = (),
) -> list[str]:
    """상태 이력 섹션. 조회 시점이 분석 구간 밖이면 그 사실을 먼저 밝힌다.

    ``cluster_health``는 ES 실시간 API라 **과거 상태를 모른다.** 그래서 과거
    사고를 분석할 때 이 섹션의 시각은 사고 시각이 아니라 진단을 돌린 시각이다.
    실측에서 9/10 15:27 사고를 분석한 리포트에 ``11:40 status=green``이 실렸고,
    그대로 두면 운영자는 사고 당시가 green이었다고 읽는다.

    값을 숨기지는 않는다. 지금 green이라는 것도 사실이고, 사고 뒤 회복됐다는
    근거가 된다 — 다만 무엇의 사실인지를 분명히 한다.
    """
    if not points:
        return []

    lines: list[str] = []
    if requested:
        window_start = min(start for start, _end in requested)
        window_end = max(end for _start, end in requested)
        outside = [p for p in points if not (window_start <= p.at <= window_end)]
        if outside:
            lines.append(
                "주의: ES는 과거 클러스터 상태를 보관하지 않는다. 아래는 진단을 "
                "돌린 시점의 상태이며, 분석 구간"
                f"({_ymd_hms(window_start)} ~ {_hms(window_end)})의 상태가 아니다."
            )
            lines.append("")

    lines += [health_line(point) for point in points]
    return lines


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
    lines.append(severity_line(obs))
    return lines


# HTML 쪽이 이 줄을 골라 배지를 붙인다. 위치로 찾으면 개요에 줄이 하나
# 늘어날 때 조용히 엉뚱한 줄이 배지를 받는다.
SEVERITY_PREFIX = "관측된 이상 신호"


def severity_line(obs: Observations) -> str:
    """관측값에서 따라 나온 심각도 한 줄. **모델 판단과 구분해 적는다.**

    "코드 판정"이라고 못 박는 이유: 이 값은 판단이 아니라 측정의 결과다.
    모델의 severity와 나란히 놓이면 운영자가 둘을 같은 것으로 읽는데, 둘이
    어긋나는 경우가 오히려 읽을 거리다.

    근거를 반드시 붙인다. 근거 없는 등급은 그 자체로 이 저장소가 없애려는
    "숫자를 옮겨 적은 판단"과 같은 것이 된다.
    """
    level, reasons = observed_severity(obs)
    if not level:
        return f"{SEVERITY_PREFIX}: 없음 (코드 판정)"
    return f"{SEVERITY_PREFIX}: {level} (코드 판정) — {', '.join(reasons)}"


_SEPARATOR = "─" * 40


def scrub(text: str) -> str:
    """UTF-8로 인코딩할 수 없는 문자를 치환한다.

    provider 응답에 짝 없는 서로게이트가 섞여 오는 경우가 있다(JSON의
    ``\\udXXX`` 이스케이프). 그대로 두면 두 경로가 동시에 무너진다 —
    ``write_text``가 ``UnicodeEncodeError``로 실패하고, 폴백으로 전문을
    로그에 남기려 해도 파일 핸들러가 같은 이유로 실패해 리포트가 통째로
    사라진다. 글자 하나를 ``?``로 바꾸는 편이 진단을 잃는 것보다 낫다.

    치환은 입력 시점에 한 번만 한다. 렌더 결과와 원문 블록, 폴백 로그가
    모두 같은 문자열에서 나오므로 여기서 걸러야 전부 안전해진다.
    """
    return text.encode("utf-8", "replace").decode("utf-8")


def render_text(report: DiagnosisReport) -> str:
    """리포트 전체를 평문 한 장으로.

    로그 폴백과 HTML의 원문 블록이 이것을 쓴다. HTML 본문은 같은 줄
    함수들로 따로 조립하지만 내용은 같다.

    섹션 번호는 코드가 센다. 제목에 박아 두면 빈 섹션 하나가 건너뛰어질 때
    6 다음이 8이 되고, HTML 쪽(``_sections_from_report``)은 처음부터 다시 번호를
    매기므로 같은 섹션이 두 출력에서 다른 번호로 불린다.
    """
    obs = report.observations
    out: list[str] = []
    numbered = [0]

    def add(title: str, lines: list[str]) -> None:
        if not lines:
            return
        numbered[0] += 1
        out.extend([f"{numbered[0]}. {title}", _SEPARATOR, *lines, ""])

    add("인시던트 개요", overview_lines(obs))
    add("분 단위 타임라인 (관측값)", [timeline_line(row) for row in obs.timeline])
    add("클러스터 상태 이력 (관측값)", health_lines(obs.health, obs.requested))
    add("노드별 구간 최대값 (관측값)", node_lines(obs.nodes))
    add(
        "마스터 노드 로그 (관측값)",
        master_log_lines(obs.master_events, obs.master_log_total),
    )

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
    add("느린 요청 후보 (관측값 + 모델 선정)", candidate_lines)

    narrative = report.narrative
    if narrative is not None:
        add("결론", [narrative.headline, *narrative.context] if narrative.headline else [])
        findings = []
        for finding in narrative.findings:
            # severity가 비면 모델이 분류하지 않은 것이다. ": 제목"으로
            # 그리면 빈 앞머리가 오타처럼 보이므로 표식을 붙인다. 코드가
            # 대신 채우지 않는 이유는 Finding docstring에 있다.
            label = finding.severity or "(모델이 분류하지 않음)"
            findings.append(f"{label}: {finding.title}")
            findings += [f"    - {item}" for item in finding.evidence]
        add("발견된 문제점", findings)
        cause = []
        if narrative.root_cause:
            cause.append(narrative.root_cause)
        cause += [f"근거: {item}" for item in narrative.supporting]
        cause += [f"반박 근거: {item}" for item in narrative.contradicting]
        cause += [f"확인하지 못한 것: {item}" for item in narrative.unverified]
        add("근본 원인", cause)
        add("권장 조치", list(narrative.recommendations))
    elif report.narrative_text:
        add("모델 리포트 (평문)", report.narrative_text.splitlines())

    return "\n".join(out).rstrip() + "\n"
