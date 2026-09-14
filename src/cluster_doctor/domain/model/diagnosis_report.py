"""진단 리포트 한 건의 최종 형태.

예전에는 리포트가 문자열 하나였다. 모델이 평문을 쓰고 ``HtmlFileNotifier``가
정규식으로 훑어 HTML을 만들었다. 그 경로가 실측에서 두 번 틀렸다.

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
(``llm/deepagent/report_schema.py``)에 있고, 그쪽이 ``to_domain()``으로 여기
타입으로 옮긴다. 매핑 함수 하나가 그 경계를 지키는 값이다 — 없애면 도메인이
pydantic과 langchain의 스키마 규약에 묶이고, provider를 바꾸는 순간 도메인이
흔들린다.
"""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal


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
class HealthPoint:
    """클러스터 상태가 유지된 한 구간.

    ``cluster_health``는 2단계 대기 루프에서 여러 번 불린다. 호출마다 한 줄을
    쌓으면 같은 green이 열 줄 늘어서고, 그 목록은 "상태 변화를 시간순으로"라는
    리포트의 요구를 오히려 가린다. 그래서 직전과 같은 상태면 새 항목을 만들지
    않고 ``until``만 늘린다.
    """

    at: datetime
    until: datetime
    status: str
    unassigned_shards: int = 0
    active_shards: int = 0
    number_of_nodes: int = 0


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

    ``analyze_logs``는 한 진단에서 여러 번 불릴 수 있으므로 값들이 누적된다.
    누적 규칙은 수집하는 쪽(``deepagent/tools.py``)에 있고, 여기 도착할 때는
    이미 병합이 끝나 있다.
    """

    time_basis: str = ""
    first_seen: datetime | None = None
    last_seen: datetime | None = None
    # analyze_logs가 실제로 요청한 구간들. 중복을 허용한다 — 같은 구간을 다시
    # 부른 것은 그 자체로 사실이다.
    requested: tuple[tuple[datetime, datetime], ...] = ()
    total_wait_seconds: float = 0.0
    wait_cap_reached: bool = False
    timeline: tuple[TimelineRow, ...] = ()
    nodes: tuple[NodeMetricRow, ...] = ()
    # 렌더된 마스터 로그 줄. 상한에 걸렸는지 보이려고 전체 건수를 따로 든다.
    master_logs: tuple[str, ...] = ()
    master_log_total: int = 0
    health: tuple[HealthPoint, ...] = ()
    candidates: tuple[SlowCandidate, ...] = ()

    def is_empty(self) -> bool:
        """관측한 것이 하나도 없는가.

        폴백 사다리의 마지막 칸을 판정한다. 여기까지 비어 있으면 그때만
        예외를 올린다 — 그 전에는 모델이 아무 말도 남기지 못했더라도
        관측값만으로 리포트가 성립한다.
        """
        return not (self.timeline or self.nodes or self.master_logs or self.health)


@dataclass(frozen=True)
class Finding:
    """모델이 지목한 문제 하나.

    ``severity``가 필드인 것이 요점이다. 예전에는 모델이 ``"Critical: …"``로
    쓰고 notifier가 정규식으로 앞머리를 떼어 배지를 붙였다. 이제는 값으로 온다.
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
