"""Triage 그래프 안을 흐르는 값들.

``RawRecord``가 이 설계의 축이다. 모델은 **번호만** 돌려주고 시각·노드·원문은
코드가 이 레코드에서 옮긴다. 모델이 로그 줄을 옮겨 적게 하면 틀린다는 것을 이
저장소는 실측으로 두 번 확인했다(``contracts/observations.py`` 모듈 docstring) —
Evidence 단계에서 같은 함정을 다시 파지 않는다.
"""

from __future__ import annotations

import operator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, TypedDict

from cluster_doctor.contracts.evidence import Evidence


@dataclass(frozen=True)
class RawRecord:
    """Triage 대상 한 줄. datasource가 무엇이든 같은 모양이 된다.

    ``record_id``는 이 워크플로 실행 안에서만 유효한 번호다. Evidence에 붙는
    영구 id와 다르다 — 그쪽은 Incident 전체에서 유일해야 하므로 저장소가
    발급한다.
    """

    record_id: int
    event_time: datetime
    line: str
    node_id: str | None = None
    node_name: str | None = None
    severity: str | None = None

    def as_prompt_line(self) -> str:
        """프롬프트에 실을 한 줄. 모델이 이 번호로 고른다."""
        return f"#{self.record_id} {self.line}"


@dataclass(frozen=True)
class MinuteBucket:
    """1분 구간과 그 안의 레코드 전량.

    샘플링하지 않는다. 이 그래프가 존재하는 이유가 그것 — 단발 호출은 소스당
    상한에서 잘리고, 잘린 쪽이 오래된 구간이라 시작점이 통째로 사라진다.
    1분치는 그 상한 아래이므로 잘리지 않는다.
    """

    minute: datetime
    records: list[RawRecord]


@dataclass(frozen=True)
class SelectedRecord:
    """Map 단계가 남긴 후보 하나."""

    record_id: int
    event_type: str = ""
    reason: str = ""


@dataclass(frozen=True)
class MinuteResult:
    """한 분의 Map 결과.

    ``failed``가 True면 그 분의 LLM 호출이 실패한 것이다. 한 분이 rate limit에
    걸렸다고 나머지 분의 결과까지 버리지 않는다. 전부 실패했을 때의 판단은
    호출자(SubAgent)가 한다 — 그쪽만이 다른 datasource의 결과까지 보고 "분석이
    성립했는가"를 말할 수 있다.
    """

    minute: datetime
    selected: list[SelectedRecord] = field(default_factory=list)
    failed: bool = False
    record_count: int = 0


class TriageState(TypedDict):
    """노드 사이를 오가는 상태.

    ``minute_results``에 ``operator.add``를 붙인 이유: 분별 노드는 팬아웃으로
    동시에 여러 개가 돌고 각자 자기 결과 하나만 담은 리스트를 돌려준다.
    reducer가 없으면 마지막에 끝난 노드가 나머지를 덮어쓴다.
    """

    records: list[RawRecord]
    buckets: list[MinuteBucket]
    minute_results: Annotated[list[MinuteResult], operator.add]
    evidence: list[Evidence]
    # Reduce가 LLM 없이 끝났는가. 근거는 남았지만 "반복·정상 이벤트 제거"가
    # 수행되지 않았다는 뜻이라 호출자가 gap으로 남긴다.
    reduce_degraded: bool
