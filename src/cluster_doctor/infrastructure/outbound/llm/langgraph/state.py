"""그래프 상태와 그 안을 흐르는 값들."""

import operator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, TypedDict

from cluster_doctor.domain.model.log_entry import LogEntry
from cluster_doctor.domain.model.time_range import TimeRange


@dataclass(frozen=True)
class MinuteBucket:
    """1분 구간과 그 안의 로그 전량.

    ``logs``는 샘플링하지 않은 전량이다. 이 그래프가 존재하는 이유가
    바로 그것 — 단발 호출은 소스당 200건에서 잘리고, 로그가 최신순으로
    정렬돼 있어 오래된 구간이 통째로 사라진다. 1분치는 그 상한 아래이므로
    잘리지 않는다.
    """

    minute: datetime
    logs: list[LogEntry]


@dataclass(frozen=True)
class MinuteFinding:
    """한 구간의 분석 결과.

    ``evidence``는 분석의 근거가 된 로그 원문이다. 종합 단계가 실제
    쿼리문·노드명·수치를 인용할 수 있게 하려고 요약과 따로 싣는다.
    요약만 넘기면 종합 리포트가 "검색 부하가 높습니다" 수준으로 뭉개진다.

    ``failed``가 True이면 그 구간의 LLM 호출이 실패한 것이다. 한 구간이
    실패했다고 진단 전체를 버리지 않고, 종합 단계에 "이 구간은 분석하지
    못했다"고 알린다 — 빠진 사실을 감추는 것보다 낫다.
    """

    minute: datetime
    summary: str
    evidence: list[str] = field(default_factory=list)
    failed: bool = False


class GraphState(TypedDict):
    """노드 사이를 오가는 상태.

    ``findings``에 ``operator.add``를 붙인 이유: 분별 분석 노드는 팬아웃으로
    동시에 여러 개가 돌고, 각자 자기 결과 하나만 담은 리스트를 돌려준다.
    reducer가 없으면 마지막에 끝난 노드가 나머지를 덮어쓴다.
    """

    time_range: TimeRange
    logs: list[LogEntry]
    buckets: list[MinuteBucket]
    findings: Annotated[list[MinuteFinding], operator.add]
    report: str
