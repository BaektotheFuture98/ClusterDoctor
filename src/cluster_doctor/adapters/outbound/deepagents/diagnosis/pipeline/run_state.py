"""한 번의 analysis window에서 코드가 관측한 사실을 모으는 상태 객체.

**Evidence와 다르다.** Evidence는 모델이 골라낸 의미 있는 줄이고, 여기 모이는
것은 코드가 센 숫자 — 분 단위 건수, 노드별 최대값, 마스터 로그 원문, 상태 이력,
느린 요청 후보다. 둘을 나누는 이유는 신뢰의 출처가 다르기 때문이다. 관측값은
모델을 거치지 않으므로 모델이 실패해도 리포트에 남는다.

Incident 수준의 누적은 여기서 하지 않는다. 이 객체는 window 하나의 것이고,
여러 window를 합치는 것은 ``ArtifactStore.merge_observations``다 — 그래야 분석
호출이 몇 번이든 규칙이 한 벌로 유지된다.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime, timezone

from cluster_doctor.domain.diagnosis.observations import MasterEvent, NodeMetricRow, Observations, SlowCandidate, TimelineRow
from cluster_doctor.domain.diagnosis.health_point import HealthPoint
from cluster_doctor.domain.diagnosis.log_entries import LogEntry, NodeLogEntry
from cluster_doctor.domain.diagnosis.time_range import TimeRange
from cluster_doctor.domain.diagnosis.kst import KST
from cluster_doctor.adapters.outbound.deepagents.diagnosis.pipeline.log_format import format_log_line
from cluster_doctor.adapters.outbound.deepagents.diagnosis.pipeline.observations import (
    candidate_key,
    merge_node_rows,
    node_metric_summary,
    slow_candidates,
    timeline_row,
)
from cluster_doctor.adapters.outbound.deepagents.diagnosis.pipeline.datasource.node_log import (
    ES_LOG_LINE_RE,
)
from cluster_doctor.application.candidate_formatter import candidate_line

_logger = logging.getLogger(__name__)

# 한 window에서 뽑는 느린 요청 후보 수. 모델은 이 목록에서 id로 고르기만
# 하므로 너무 많으면 고르는 일 자체가 어려워지고, 너무 적으면 진짜 원인이
# 목록 밖으로 밀려난다.
CANDIDATES_PER_WINDOW = 5

# 리포트에 싣는 마스터 로그 줄 수 상한. 한 줄이 평균 524자라 그대로 실으면
# HTML이 수백 KB가 된다. 잘린 사실은 ``master_log_total``로 드러난다.
MASTER_LOG_REPORT_MAX = 120

# 마스터 로그 정렬의 기준점. SSH 폴백으로 온 줄은 timestamp를 뽑을 수 없어
# None인데, None과 datetime을 직접 비교하면 TypeError가 난다.
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class AnalysisRunState:
    """window 하나의 관측값과 빠진 근거."""

    def __init__(self, window: TimeRange, time_basis: str = "") -> None:
        self.window = window
        self.time_basis = time_basis

        self.timeline: dict[datetime, TimelineRow] = {}
        self.nodes: dict[str, NodeMetricRow] = {}
        self.master_logs: dict[object, MasterEvent] = {}
        self.health: list[HealthPoint] = []
        self.candidates: dict[object, SlowCandidate] = {}

        # 수집하지 못한 보조 근거. 리포트는 유효하지만 일부가 빠졌다는 사실이
        # 운영자에게 반드시 도달해야 한다 — notifier가 배너로 그린다.
        self.gaps: list[str] = []
        # 분석 자체가 성립하지 않았는가. 리포트를 버리지는 않지만 재트리거를
        # 막는 유일한 조건이다.
        self.degraded = False

    def mark_gap(self, observation: str) -> str:
        """근거가 일부 빠졌다는 사실을 남긴다. 리포트는 버리지 않는다.

        ``degraded``와 나누는 기준은 "분석이 성립했는가"다. 보조 조사가 실패해도
        주 분석 결과는 온전하므로 리포트를 버릴 이유가 없다.
        """
        self.gaps.append(observation)
        _logger.info("[analysis] gap: %s", observation)
        return observation

    def record_master_logs(self, entries: list[NodeLogEntry]) -> None:
        """ClickHouse에서 온 마스터 로그를 기록한다. 중복은 내용으로 거른다.

        렌더된 문자열이 아니라 구조로 담는다. 리포트가 같은 사건끼리 묶어야
        하는데, 묶으려면 ``logger``가 값으로 있어야 한다.
        """
        for entry in entries:
            key = (entry.timestamp, entry.node, entry.line)
            if key not in self.master_logs:
                self.master_logs[key] = MasterEvent(
                    timestamp=entry.timestamp,
                    node=entry.node,
                    level=(entry.level or entry.detected_level or "").strip(),
                    logger=(entry.logger or "").strip(),
                    line=entry.line,
                    rendered=format_log_line(entry),
                )

    def record_master_text(self, text: str) -> None:
        """SSH 폴백으로 온 마스터 로그. 줄 자체가 키다.

        ES 로그 줄에서 시각·레벨·로거를 뽑는다. 세 칸을 비워 두면 리포트가
        사건별로 묶을 때 쓰는 키가 모든 줄에 대해 같은 값이 되어 수백 줄이
        헤더 한 줄로 붕괴한다.

        뽑지 못한 줄(스택 트레이스 연속 행 등)은 버리지 않는다. 값이 없다는
        것과 줄이 없다는 것은 다르고, 렌더러가 그런 줄을 따로 다룬다.
        """
        for line in text.splitlines():
            if not line.strip() or line in self.master_logs:
                continue
            timestamp = None
            level = ""
            logger_name = ""
            match = ES_LOG_LINE_RE.match(line)
            if match:
                level = match.group(2).strip()
                logger_name = match.group(3).strip()
                try:
                    timestamp = datetime.fromisoformat(match.group(1)).replace(tzinfo=KST)
                except ValueError:
                    timestamp = None
            self.master_logs[line] = MasterEvent(
                timestamp=timestamp,
                level=level,
                logger=logger_name,
                line=line,
                rendered=line,
            )

    def record_candidates(self, entries: list[LogEntry]) -> None:
        """느린 요청 후보에 id를 붙여 기록한다.

        id는 발견 순서대로 한 번만 붙는다. 겹친 구간을 다시 조회해 같은 요청이
        또 나와도 새 번호를 주지 않는다 — 모델이 이미 본 id가 가리키는 것이
        중간에 바뀌면 안 된다.
        """
        for candidate in slow_candidates(entries, limit=CANDIDATES_PER_WINDOW):
            key = candidate_key(candidate)
            if key in self.candidates:
                continue
            self.candidates[key] = replace(
                candidate, candidate_id=f"C{len(self.candidates) + 1}"
            )

    def record_log_observations(self, entries: list[LogEntry]) -> None:
        """LLM을 타지 않는 관측값을 거둔다. 여기서 예외가 새면 안 된다.

        로그를 손에 넣은 **직후에** 부른다. 노드 메트릭과 후보는 순수 함수가
        만드는 값이므로, 이 수집을 LLM 단계 뒤로 미루면 429로 분 단위 호출이
        전부 실패했을 때 **조회에 성공한 구간이 리포트에서 통째로 빈칸이 된다** —
        운영자에게는 "아무 일도 없던 시간"으로 보인다.
        """
        try:
            merge_node_rows(self.nodes, node_metric_summary(entries))
            self.record_candidates(entries)
            self.record_timeline(entries)
        except Exception:
            _logger.exception("[analysis] 관측값 수집 실패 — 분석은 계속한다")

    def record_timeline(self, entries: list[LogEntry], *, failed: bool = False) -> None:
        """분 단위 타임라인을 채운다.

        ``failed``는 그 분의 LLM 선별이 실패했다는 뜻이다. 행 자체가 없으면 그
        분이 타임라인에서 사라지고, "실패해서 못 봤다"가 "아무 일도 없었다"로
        읽힌다. 건수와 최대값은 로그만으로 계산되므로 LLM이 실패해도 정확하다.
        """
        grouped: dict[datetime, list[LogEntry]] = {}
        for entry in entries:
            minute = entry.timestamp.replace(second=0, microsecond=0)
            grouped.setdefault(minute, []).append(entry)
        for minute, bucket in grouped.items():
            # 이미 성공한 분은 덮지 않는다.
            if failed and minute in self.timeline:
                continue
            self.timeline[minute] = timeline_row(minute, bucket, failed=failed)

    def record_health(self, payload: dict, now: datetime) -> None:
        """클러스터 상태를 이력에 접어 넣는다.

        직전과 같으면 ``until``만 늘린다. 호출마다 한 줄을 쌓으면 같은 green이
        열 줄 늘어서고, 그 목록은 "상태 변화를 시간순으로"라는 리포트의 요구를
        오히려 가린다.
        """
        status = str(payload.get("status", ""))
        unassigned = int(payload.get("unassigned_shards", 0) or 0)
        if (
            self.health
            and self.health[-1].status == status
            and self.health[-1].unassigned_shards == unassigned
        ):
            self.health[-1] = replace(self.health[-1], until=now)
            return
        self.health.append(
            HealthPoint(
                at=now,
                until=now,
                status=status,
                unassigned_shards=unassigned,
                active_shards=int(payload.get("active_shards", 0) or 0),
                number_of_nodes=int(payload.get("number_of_nodes", 0) or 0),
            )
        )

    def to_observations(self) -> Observations:
        """누적된 관측값을 도메인 객체로 굳힌다.

        수집 중에는 병합이 쉬운 dict로 들고 있다가 여기서 정렬된 tuple이 된다.
        리포트에 실린 뒤에는 바뀌지 않아야 하므로 frozen 타입으로 옮긴다.
        """
        # timestamp가 있는 줄을 먼저, 시간순으로. SSH 폴백 줄(timestamp 없음)은
        # 뒤로 보내되 넣은 순서를 유지한다 — 원본 파일의 순서가 곧 시간순이다.
        ordered_master = sorted(
            enumerate(self.master_logs.values()),
            key=lambda pair: (
                pair[1].timestamp is None,
                pair[1].timestamp or _EPOCH,
                pair[0],
            ),
        )
        master_events = tuple(event for _index, event in ordered_master)

        return Observations(
            time_basis=self.time_basis,
            requested=((self.window.start, self.window.end),),
            timeline=tuple(self.timeline[minute] for minute in sorted(self.timeline)),
            nodes=tuple(sorted(self.nodes.values(), key=lambda row: row.node)),
            master_events=master_events[:MASTER_LOG_REPORT_MAX],
            master_log_total=len(master_events),
            health=tuple(self.health),
            # id는 C1, C2 … 순으로 붙었으므로 발견 순서로 정렬하려면 숫자 부분을
            # 봐야 한다. 문자열 정렬이면 C10이 C2 앞에 온다. int()로 파싱하지
            # 않는 이유는 여기서 예외가 나면 리포트 전달 자체가 깨지기 때문이다 —
            # 길이를 먼저 보면 파싱 없이 같은 순서가 나오고 어떤 문자열이 와도
            # 터지지 않는다.
            candidates=tuple(
                sorted(
                    self.candidates.values(),
                    key=lambda c: (len(c.candidate_id), c.candidate_id),
                )
            ),
        )

    def candidate_ids(self) -> set[str]:
        """지금까지 붙인 후보 id 전부. 검증이 이 집합과 대조한다."""
        return {item.candidate_id for item in self.candidates.values()}

    def candidates_for_prompt(self) -> str:
        """느린 요청 후보를 id와 함께 프롬프트에 실을 블록으로.

        후보 줄은 운영자용 리포트와 **같은 함수**로 그린다. 여기서 따로 그리면
        모델이 보는 수치와 리포트에 실리는 수치가 갈린다 — 실측으로
        es_query_log 264건이 slowlog로 실린 적이 있다.
        """
        if not self.candidates:
            return ""
        ordered = sorted(
            self.candidates.values(),
            key=lambda c: (len(c.candidate_id), c.candidate_id),
        )
        lines = "\n".join(candidate_line(item) for item in ordered)
        return (
            "느린 요청 후보 (코드가 골랐다 — id와 수치를 그대로 쓴다. "
            "문제로 보이는 것이 있으면 suspect_picks에 **id와 이유만** 쓴다):\n"
            f"{lines}"
        )

    def summary_for_prompt(self) -> str:
        """모델에게 보여 줄 관측값 요약.

        **옮겨 적으라고 주는 것이 아니다.** 리포트의 수치 칸은 코드가 채우고,
        이것은 모델이 "그 시각이 조용했는지 시끄러웠는지"를 알기 위한 배경이다.
        """
        lines: list[str] = []
        for minute in sorted(self.timeline):
            row = self.timeline[minute]
            counts = ", ".join(f"{key}={value}" for key, value in sorted(row.counts.items()))
            extra = []
            if row.took_max:
                extra.append(f"took_max={row.took_max}")
            if row.jvm_heap_max is not None:
                extra.append(f"jvm_heap_max={row.jvm_heap_max}%({row.jvm_heap_max_node})")
            if row.search_rejected_max or row.write_rejected_max:
                extra.append(
                    f"rejected(search={row.search_rejected_max}, write={row.write_rejected_max})"
                )
            marker = " [선별 실패]" if row.failed else ""
            lines.append(
                f"{minute:%H:%M}{marker} {counts}" + (f" | {' '.join(extra)}" if extra else "")
            )
        if self.health:
            last = self.health[-1]
            lines.append(
                f"클러스터 상태(조회 시점): {last.status} "
                f"unassigned={last.unassigned_shards} nodes={last.number_of_nodes}"
            )
        return "\n".join(lines) or "(관측값 없음)"
