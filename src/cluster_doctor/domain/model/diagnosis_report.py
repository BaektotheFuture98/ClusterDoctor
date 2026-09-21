"""진단 리포트 한 건의 최종 형태.

리포트가 문자열 하나이면 모델이 평문을 쓰고 notifier가 정규식으로 훑어야 한다.
그 경로는 실측에서 두 번 틀렸다.

  1. 타임라인이 ``slowlog=264``라고 썼는데 그 구간의 실제 slowlog는 0건이고
     264는 ``es_query_log`` 건수였다. 코드는 소스별 건수를 정확히 세어
     넘겼지만(``MinuteFinding.counts``) 프롬프트의 줄 형식에 소스 칸이 하나뿐이라
     모델이 비어 있지 않은 숫자를 그 칸에 넣었다.
  2. "문제 쿼리 후보"의 ``took``·``total_hits``가 전부 ``미확인``이었다. 그 값은
     ``SlowlogEntry``에만 있는데 해당 구간 slowlog가 0건이라 모델이
     ``es_query_log`` 항목을 고르고 칸을 채우지 못했다.

둘 다 뿌리가 같다 — **코드가 이미 아는 값을 모델이 옮겨 적게 시켰다.**
이 코드베이스는 같은 위험 때문에 ``MinuteFinding.counts``를 코드가 세게 했고
(*"모델이 옮겨 적다 틀리면 운영자가 잘못된 건수를 근거로 판단한다"*),
``gaps``도 코드가 기록하게 했다. 이 모듈은 그 원칙을 최종 리포트까지 밀어
올린다.

경계가 둘이다.

  ``Observations``  코드가 관측한 사실. 모델을 거치지 않는다.
  ``Narrative``     모델의 판단. 근거는 관측값에서 인용한다.

``Narrative``가 ``None``일 수 있는 것이 중요하다. 구조화 출력이 실패해도
관측값 섹션은 그대로 렌더되어야 한다 — 이 저장소의 "리포트는 항상 전달된다"
원칙이 그것을 요구한다. 그래서 포트는 합집합 타입(``DiagnosisReport | str``)을
갖지 않는다. "구조화됐는가"는 이 객체 **안에서** 표현된다.

도메인이므로 dataclass만 쓴다. 모델이 채우는 pydantic 스키마는 어댑터 계층
(``agent/diagnosis/schema.py``)에 있고, 그쪽이 ``to_domain()``으로 여기
타입으로 옮긴다. 매핑 함수 하나가 그 경계를 지키는 값이다 — 없애면 도메인이
pydantic과 langchain의 스키마 규약에 묶이고, provider를 바꾸는 순간 도메인이
흔들린다.
"""

from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal

from cluster_doctor.domain.model.elasticsearch.health_point import HealthPoint


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
    카운터다. 그래서 최댓값이 곧 구간 말 값이고, 구간 내 증가분이 아니다.
    리포트에서 "0이 아니다"만 근거로 쓰는 이유가 그것이다.
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


@dataclass(frozen=True)
class Observations:
    """코드가 관측한 사실 전부. 모델을 거치지 않는다.

    한 Incident 안에서 분석이 여러 번 일어날 수 있으므로 값들이 누적된다.
    누적 규칙은 수집하는 쪽(``agent/diagnosis/run_state.py``와
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

    def is_empty(self) -> bool:
        """관측한 것이 하나도 없는가.

        폴백 사다리의 마지막 칸을 판정한다. 여기까지 비어 있으면 그때만
        예외를 올린다 — 그 전에는 모델이 아무 말도 남기지 못했더라도
        관측값만으로 리포트가 성립한다.
        """
        return not (self.timeline or self.nodes or self.master_events or self.health)


def observed_severity(obs: Observations) -> tuple[str, tuple[str, ...]]:
    """관측값만으로 심각도를 판정한다. 근거를 함께 돌려준다.

    **모델의 severity를 대신하지 않는다.** 이것은 판단이 아니라 측정에서
    기계적으로 따라 나오는 값이고, 리포트에도 "코드 판정"으로 따로 실린다.
    두 값이 어긋나면 그것 자체가 읽을 거리다 — 모델이 Info라고 한 구간에서
    코드가 rejected를 셌다면 모델의 판단을 의심할 근거가 된다.

    필요한 이유는 실측이다. 모델이 ``severity``를 채우지 않는 일이 반복됐고
    (프롬프트에 "반드시 고른다"를 두 군데 넣은 뒤에도 그랬다), 그래서 노드
    이탈과 GREEN→YELLOW 전환이 분류 없이 리포트에 실렸다. 기본값을
    ``"Info"``로 되돌리는 것은 이미 실패한 길이다 — 25초 지연과 노드 19대
    타임아웃이 전부 Info로 나왔었다. 빈칸을 그럴듯한 값으로 채우는 대신,
    **코드가 아는 사실로 말한다.**

    판정 규칙은 전부 구조화된 필드에서 온다. 모델이 쓴 문장을 읽지 않는다 —
    읽기 시작하면 모델 산문 파싱이 되고, 그것이 정확히 이 저장소가 두 번
    당한 실패다.

      Critical  rejected > 0        요청이 실제로 거절됐다. 사용자가 받은 오류다
      Warning   마스터 ERROR        클러스터 이벤트가 오류 수준으로 찍혔다
                분석 실패한 분      그 시각의 근거가 리포트에 없다
      Info      마스터 WARN         임계값 초과 경고. 흔하지만 무시할 값은 아니다
      (없음)    위 어느 것도 아님

    **클러스터 상태(health)는 규칙에 넣지 않는다.** ``cluster_health``는 ES
    실시간 API라 과거를 모르고, 과거 사고를 분석하면 그 값은 진단을 돌린
    시점의 상태다(리포트도 그렇게 경고한다). 분석 구간 안에 들어오는 관측만
    센다 — 밖의 값으로 심각도를 매기면 사고와 무관한 시각의 green이 "정상"
    판정을 만든다.
    """
    reasons: list[str] = []
    level = ""

    rejected = sum(
        row.search_rejected_max + row.write_rejected_max for row in obs.nodes
    ) or sum(
        row.search_rejected_max + row.write_rejected_max for row in obs.timeline
    )
    if rejected:
        level = "Critical"
        reasons.append(f"search/write rejected {rejected}건")

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
            level = "Critical" if point.status.lower() == "red" else (level or "Warning")
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


@dataclass(frozen=True)
class Finding:
    """모델이 지목한 문제 하나.

    ``severity``가 필드인 것이 요점이다. 모델이 ``"Critical: …"``처럼 본문에
    섞어 쓰게 두면 notifier가 정규식으로 앞머리를 떼야 하고, 본문에 우연히 들어간
    "Info"까지 배지로 승격된다.

    비어 있을 수 있다. 그때는 렌더러가 "모델이 분류하지 않음"으로 그린다 —
    코드가 대신 채우지 않는다. 이 필드는 **모델의 판단**이고, 관측값에서
    따라 나오는 심각도는 ``observed_severity``가 따로 낸다.
    """

    severity: str
    title: str
    evidence: tuple[str, ...] = ()


@dataclass(frozen=True)
class SuspectPick:
    """모델이 후보 중 고른 것. 수치는 ``candidate_id``로 조인해 붙인다."""

    candidate_id: str
    reason: str = ""


@dataclass(frozen=True)
class Narrative:
    """모델의 판단.

    관측값은 하나도 담지 않는다. 담으면 모델이 옮겨 적어야 하고, 옮겨 적으면
    틀린다 — 이 모듈 docstring의 결함 둘이 그것이다.
    """

    headline: str = ""
    context: tuple[str, ...] = ()
    findings: tuple[Finding, ...] = ()
    root_cause: str = ""
    supporting: tuple[str, ...] = ()
    contradicting: tuple[str, ...] = ()
    unverified: tuple[str, ...] = ()
    suspect_picks: tuple[SuspectPick, ...] = ()
    recommendations: tuple[str, ...] = ()


@dataclass(frozen=True)
class DiagnosisReport:
    """운영자에게 전달되는 리포트 한 건.

    ``narrative``와 ``narrative_text``는 배타적이다.

      narrative 있음        구조화 출력 성공. 정상 경로.
      narrative_text 있음   구조화는 실패했지만 모델이 평문은 남겼다.
                            notifier가 기존 정규식 파서로 그린다.
      둘 다 없음            모델이 아무것도 남기지 못했다. 관측값만 그린다.

    셋 중 어느 경우든 ``observations``는 그대로 렌더된다. 그것이 이 설계의
    요점이다 — 모델이 실패해도 운영자는 그 시각에 무슨 일이 있었는지 본다.
    """

    observations: Observations
    narrative: Narrative | None = None
    narrative_text: str = ""


def merge_node_row(current: NodeMetricRow, new: NodeMetricRow) -> NodeMetricRow:
    """같은 노드의 두 관측을 합친다. 지표마다 max의 max, 표본은 합.

    쌍 단위 병합을 도메인에 두는 이유: 이 값을 합치는 곳이 둘이다. 한 번의
    분석 안에서 구간을 합칠 때(``observations.merge_node_rows``)와, 한 Incident
    안에서 여러 번의 분석을 합칠 때(``ArtifactStore.merge_observations``).
    규칙이 두 벌이 되면 한쪽만 고쳐지는 날이 온다.
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
        candidates=tuple(
            sorted(candidates.values(), key=lambda c: (len(c.candidate_id), c.candidate_id))
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
