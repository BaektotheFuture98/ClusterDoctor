"""진단 한 건이 관측한 사실을 모으는 상태 객체.

tool은 실패를 예외가 아니라 문자열로 돌려주므로(예외는 agent 실행 전체를
죽인다) 무엇을 보았고 무엇을 놓쳤는지가 반환값만으로는 호출자에게 닿지
않는다. 이 객체가 그 통로다 — tool이 쓰고 ``DeepAgentAnalyzer``가 읽는다.

키 문자열이 아니라 속성으로 두는 이유는 두 모듈이 같은 상태를 공유하기
때문이다. 문자열로 주고받으면 오타가 조용한 빈 값이거나 tool 안의
``KeyError``가 되고, tool에서 샌 예외는 agent 실행 전체를 죽인다.

누적 구조가 전부 **키 기반 dict**인 것이 요점이다. ``analyze_logs``는 한
진단에서 여러 번 불리고 구간이 겹칠 수 있는데, 리스트에 이어 붙이면 같은
분·같은 노드·같은 로그 줄이 두 번 실린다. ``health``만 리스트인 것은 그것이
시간순 이력이기 때문이고, 대신 직전과 같은 상태면 항목을 늘리지 않고 접는다.
"""
from __future__ import annotations

import logging
import re
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from cluster_doctor.domain.model.diagnosis_report import (
    HealthPoint,
    MasterEvent,
    NodeMetricRow,
    Observations,
    SlowCandidate,
    TimelineRow,
)
from cluster_doctor.domain.model.log_entry import LogEntry, NodeLogEntry
from cluster_doctor.infrastructure.outbound.agent.common.time_window import (
    KST,
    fmt,
    merge_intervals,
)
from cluster_doctor.infrastructure.outbound.agent.common.observations import (
    candidate_key,
    merge_node_rows,
    node_metric_summary,
    slow_candidates,
    timeline_row,
)
from cluster_doctor.infrastructure.outbound.agent.common.log_format import format_log_line
# 후보 줄은 운영자용 리포트와 **같은 함수**로 그린다. 여기서 따로 그리면
# 모델이 보는 수치와 리포트에 실리는 수치가 갈린다 — 실측으로 es_query_log
# 264건이 slowlog로 실린 적이 있다.
from cluster_doctor.infrastructure.outbound.notifier.report_text import candidate_line

_logger = logging.getLogger(__name__)

# ES 로그 한 줄의 머리: [시각][레벨][로거]. SSH 폴백은 파일 원문을 그대로
# 받으므로 여기서 뽑지 않으면 레벨과 로거가 리포트에 도달하지 못한다.
# 로거 이름은 오른쪽이 공백으로 채워져 있다(``[o.e.c.c.C          ]``).
_ES_LOG_LINE_RE = re.compile(
    r"^\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})[,.]\d+\]"
    r"\[([A-Z ]+)\]"
    r"\[([^\]]+)\]"
)

# analyze_logs 호출 한 번이 소스마다 내놓는 느린 요청 후보 수. 모델은 이
# 목록에서 id로 고르기만 하므로 너무 많으면 고르는 일 자체가 어려워지고,
# 너무 적으면 진짜 원인이 목록 밖으로 밀려난다.
_CANDIDATES_PER_CALL = 5

# 파이프라인 지연을 의심하는 문턱.
_PIPELINE_DELAY_LIMIT = timedelta(minutes=30)

# 커버리지 판정의 허용오차. agent는 구간을 분 경계로 반올림하므로 유입 마지막
# 몇십 초가 구간 밖으로 밀려나는 일이 정상적으로 생긴다. 그것까지 누락으로
# 보고하면 배너가 잡음이 되고, 잡음이 된 배너는 읽히지 않는다.
_COVERAGE_TOLERANCE = timedelta(seconds=60)

# 리포트에 싣는 마스터 로그 줄 수 상한. 수집 쪽 상한이 analyze_logs 호출마다
# 걸리므로 최대 6회면 480줄까지 쌓인다. 한 줄이 평균 524자라 그대로 실으면
# HTML이 수백 KB가 된다. 잘린 사실은 master_log_total로 드러난다.
_MASTER_LOG_REPORT_MAX = 120

# 마스터 로그 정렬의 기준점. SSH 폴백으로 온 줄은 timestamp를 뽑을 수 없어
# None인데, None과 datetime을 직접 비교하면 TypeError가 난다.
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _base_time(
    log_time: datetime, kafka_receive_time: datetime
) -> tuple[datetime, str]:
    """유입 시작 시각의 기준을 정한다.

    돌려주는 문자열은 리포트의 "사용한 시각 기준" 필드로 그대로 간다. 그래야
    그 값이 모델의 주장이 아니라 관측된 사실이 된다.
    """
    if log_time > kafka_receive_time:
        # clock skew. slowlog가 수신보다 미래일 수는 없다.
        return kafka_receive_time, "kafka_receive_time (clock skew)"
    if kafka_receive_time - log_time > _PIPELINE_DELAY_LIMIT:
        return kafka_receive_time, "kafka_receive_time (파이프라인 지연 30분 초과)"
    return log_time, "slowlog_timestamp"


class DiagnosisState:
    """tool이 쓰고 어댑터가 읽는 진단 실행 상태."""

    def __init__(self, log_time: datetime, kafka_receive_time: datetime) -> None:
        base, basis = _base_time(log_time, kafka_receive_time)

        self.timeline: dict[datetime, TimelineRow] = {}
        self.nodes: dict[str, NodeMetricRow] = {}
        self.master_logs: dict[object, MasterEvent] = {}
        self.health: list[HealthPoint] = []
        self.candidates: dict[object, SlowCandidate] = {}
        self.wait_seconds: float = 0.0
        self.wait_cap_reached: bool = False

        self.first_seen: datetime | None = base
        self.last_seen: datetime | None = None
        self.zero_streak: int = 0
        self.time_basis: str = basis

        # analyze_logs가 실제로 요청한 (start, end). 커버리지 판정의 입력.
        self.requested: list[tuple[datetime, datetime]] = []
        # 종합까지 성공한 구간 / 실패한 구간. 실패가 나중에 성공으로 덮이면
        # 없던 일이 된다(unresolved_failure).
        self.analyzed: list[tuple[datetime, datetime]] = []
        self.failed: list[tuple[datetime, datetime]] = []

        self.degraded: bool = False
        self.gaps: list[str] = []

    def record_master_logs(self, entries: list[NodeLogEntry]) -> None:
        """ClickHouse에서 온 마스터 로그를 기록한다. 중복은 내용으로 거른다.

        렌더된 문자열이 아니라 구조로 담는다. 리포트가 같은 사건끼리 묶어야
        하는데, 묶으려면 logger가 값으로 있어야 한다.
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
            match = _ES_LOG_LINE_RE.match(line)
            if match:
                level = match.group(2).strip()
                logger_name = match.group(3).strip()
                try:
                    timestamp = datetime.fromisoformat(match.group(1)).replace(
                        tzinfo=KST
                    )
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
        for candidate in slow_candidates(entries, limit=_CANDIDATES_PER_CALL):
            key = candidate_key(candidate)
            if key in self.candidates:
                continue
            self.candidates[key] = replace(
                candidate, candidate_id=f"C{len(self.candidates) + 1}"
            )

    def record_log_observations(self, entries: list[LogEntry]) -> None:
        """LLM을 타지 않는 관측값을 거둔다. 여기서 예외가 새면 안 된다.

        이 메서드는 tool 안에서 불리고 tool에서 샌 예외는 agent 실행 전체를
        죽여 리포트까지 사라지게 하므로, 방어 비용이 세 줄이면 건다.
        """
        try:
            merge_node_rows(self.nodes, node_metric_summary(entries))
            self.record_candidates(entries)
        except Exception:
            _logger.exception("[state] 관측값 수집 실패 — 분석은 계속한다")

    def record_failed_timeline(self, entries: list[LogEntry]) -> None:
        """분석이 실패한 구간의 타임라인을 실패 표식과 함께 채운다.

        행 자체가 없으면 그 분이 타임라인에서 사라지고, "실패해서 못 봤다"가
        "아무 일도 없었다"로 읽힌다. 건수와 최대값은 로그만으로 계산되므로
        LLM이 실패해도 정확하다. 이미 성공한 분은 덮지 않는다.
        """
        if not entries:
            return
        try:
            grouped: dict[datetime, list[LogEntry]] = {}
            for entry in entries:
                minute = entry.timestamp.replace(second=0, microsecond=0)
                grouped.setdefault(minute, []).append(entry)
            for minute, bucket in grouped.items():
                if minute not in self.timeline:
                    self.timeline[minute] = timeline_row(minute, bucket, failed=True)
        except Exception:
            _logger.exception("[state] 실패 구간 타임라인 기록 실패")

    def record_health(self, payload: dict, now: datetime) -> None:
        """클러스터 상태를 이력에 접어 넣는다.

        cluster_health는 대기 루프에서 여러 번 불린다. 호출마다 한 줄을 쌓으면
        같은 green이 열 줄 늘어서고, 그 목록은 "상태 변화를 시간순으로"라는
        리포트의 요구를 오히려 가린다. 직전과 같으면 until만 늘린다.
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

    def candidate_block(self) -> str:
        """지금까지 모인 느린 요청 후보를 id와 함께 붙인다.

        누적분을 매번 다 싣는다. 후보는 호출당 상한이 있고 id가 한 번 붙으면
        바뀌지 않아 분량이 폭주하지 않으며, 마지막 호출의 반환값만 보고 고르게
        두면 앞 구간의 후보가 선택지에서 조용히 빠진다.
        """
        if not self.candidates:
            return ""
        ordered = sorted(
            self.candidates.values(),
            key=lambda c: (len(c.candidate_id), c.candidate_id),
        )
        lines = "\n".join(candidate_line(c) for c in ordered)
        return (
            "\n\n느린 요청 후보 (코드가 고른 것 — id와 수치를 그대로 쓴다):\n"
            f"{lines}"
        )

    def mark_window_failed(
        self, start_dt: datetime, end_dt: datetime, observation: str
    ) -> str:
        """이 구간 분석이 실패했음을 기록한다. 진단 실패로 확정하지는 않는다.

        같은 구간을 다시 불러 성공하면 없던 일이 된다. 이 경로의 실패는
        일시적인 경우가 많고, 그때 agent는 같은 구간을 그대로 재호출한다.
        여기서 곧바로 ``degraded``를 박으면 되돌릴 방법이 없어 재시도가
        성공해도 붉은 배너가 붙고 재트리거가 막힌다. 확정은 실행이 끝난 뒤
        ``unresolved_failure``가 한다.
        """
        self.failed.append((start_dt, end_dt))
        return observation

    def mark_gap(self, observation: str) -> str:
        """근거가 일부 빠졌다는 사실을 남긴다. 리포트는 버리지 않는다.

        ``degraded``와 나누는 기준은 "진단이 성립했는가"다. 보조 조사가
        실패해도 분석 결과는 온전하므로 리포트를 버릴 이유가 없다. 그렇다고
        조용히 넘길 수도 없다 — 여기 남긴 것은 notifier가 배너로 그리므로
        모델이 프롬프트를 어겼더라도 빠진 사실이 운영자에게 반드시 도달한다.
        """
        self.gaps.append(observation)
        return observation

    def unresolved_failure(self) -> str | None:
        """성공으로 덮이지 않은 실패 구간이 남았는지 본다.

        실패한 구간을 나중에 다시 불러 종합까지 성공했다면 진단은 성립한
        것이므로 아무 표식도 남기지 않는다. ``merge_intervals``를 쓰는 이유는
        재시도 구간이 원래 구간과 정확히 같지 않고 더 넓게 또는 쪼개져 들어올
        수 있어서다.
        """
        if not self.failed:
            return None
        covered = merge_intervals(self.analyzed)
        leftover = [
            (start, end)
            for start, end in self.failed
            if not any(c0 <= start and end <= c1 for c0, c1 in covered)
        ]
        if not leftover:
            return None
        return ", ".join(f"{fmt(s)} ~ {fmt(e)}" for s, e in leftover)

    def coverage_gaps(self) -> list[str]:
        """요청된 구간들의 합집합이 관측된 유입을 감쌌는지 본다.

        **합집합으로만 판정한다.** 호출마다 따로 보면 정당한 분할이 전부
        위반으로 잡힌다 — 20분을 두 조각으로 나누면 첫 조각은
        ``end < last_seen``이고 둘째 조각은 ``start > first_seen``인 것이
        당연하다.

        판정 대상은 관측된 유입 자체뿐이다. 앞쪽 lookback은 기본값이지 계약이
        아니므로 그것을 못 채운 것은 누락으로 보고하지 않는다.
        ``last_seen``이 없으면 아무것도 주장하지 않는다.
        """
        if self.first_seen is None or self.last_seen is None:
            return []
        if self.last_seen < self.first_seen:
            return []

        if not self.requested:
            return [
                f"유입 구간({fmt(self.first_seen)} ~ {fmt(self.last_seen)})을 "
                "분석하지 않았다 — analyze_logs가 호출되지 않았다."
            ]

        uncovered = timedelta()
        cursor = self.first_seen
        for start, end in merge_intervals(self.requested):
            if end <= cursor:
                continue
            if start > cursor:
                uncovered += min(start, self.last_seen) - cursor
            cursor = max(cursor, end)
            if cursor >= self.last_seen:
                break
        if cursor < self.last_seen:
            uncovered += self.last_seen - cursor

        if uncovered <= _COVERAGE_TOLERANCE:
            return []
        return [
            f"관측된 유입 {fmt(self.first_seen)} ~ {fmt(self.last_seen)} 중 "
            f"{int(uncovered.total_seconds())}초가 분석 구간에 포함되지 않았다."
        ]

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
            first_seen=self.first_seen,
            last_seen=self.last_seen,
            requested=tuple(self.requested),
            total_wait_seconds=float(self.wait_seconds),
            wait_cap_reached=bool(self.wait_cap_reached),
            timeline=tuple(self.timeline[minute] for minute in sorted(self.timeline)),
            nodes=tuple(sorted(self.nodes.values(), key=lambda row: row.node)),
            master_events=master_events[:_MASTER_LOG_REPORT_MAX],
            master_log_total=len(master_events),
            health=tuple(self.health),
            # id는 C1, C2 … 순으로 붙었으므로 발견 순서로 정렬하려면 숫자
            # 부분을 봐야 한다. 문자열 정렬이면 C10이 C2 앞에 온다. int()로
            # 파싱하지 않는 이유는 여기서 예외가 나면 리포트 전달 자체가
            # 깨지기 때문이다 — 길이를 먼저 보면 파싱 없이 같은 순서가 나오고
            # 어떤 문자열이 와도 터지지 않는다.
            candidates=tuple(
                sorted(
                    self.candidates.values(),
                    key=lambda c: (len(c.candidate_id), c.candidate_id),
                )
            ),
        )
