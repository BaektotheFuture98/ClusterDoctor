"""SubAgent의 제안을 실제로 새로 필요한 구간으로 바꾼다.

**차집합 산수를 모델에게 시키지 않는다.** 13:50~14:05가 제안됐고 14:00~14:10이
이미 분석됐다면 답은 13:50~14:00 하나뿐이고, 그것은 판단이 아니라 계산이다.
계산을 모델이 하면 틀리고, 틀린 것이 중복 분석이면 가장 비싼 자원(분당 LLM
호출)을 헛되이 태운다.

모델이 하는 일은 이 함수가 내놓은 후보 **중에서 무엇을 왜 볼 것인가**, 혹은 더
볼 필요가 없다는 판단이다. 그 경계가 이 모듈의 존재 이유다.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from cluster_doctor.domain.model.incident_state import IncidentState
from cluster_doctor.domain.model.time_range import (
    TimeRange,
    merge_spans,
    split_span,
    subtract_spans,
)

_logger = logging.getLogger(__name__)

# 이보다 짧게 남은 조각은 후보로 올리지 않는다. 분 경계 반올림 때문에 몇십
# 초짜리 꼬리가 정상적으로 생기는데, 그것 하나 때문에 분석 호출 하나를 쓰면
# 예산 여섯 번 중 한 번이 사라진다.
MIN_USEFUL_WINDOW = timedelta(minutes=1)


def plan_new_windows(
    suggested: "list[TimeRange] | tuple[TimeRange, ...]",
    state: IncidentState,
    *,
    limit: int = 4,
) -> list[TimeRange]:
    """제안 구간에서 아직 보지 않은 부분만 남긴다.

    제안끼리 먼저 합친다. SubAgent가 겹치는 제안을 둘 내놓으면(흔하다) 각각
    빼는 것만으로는 서로 겹친 후보 둘이 남는다.

    가까운 과거부터 돌려준다. 제안이 여럿일 때 사고 시각에 붙은 쪽이 먼저
    쓸모 있고, 예산이 모자라면 먼 쪽이 잘리는 편이 낫다.
    """
    if not suggested:
        return []

    candidates: list[TimeRange] = []
    for start, end in merge_spans(list(suggested)):
        # 합친 결과는 10분을 넘길 수 있다. 조회 팬아웃 상한은 그대로 지켜야
        # 하므로 여기서 다시 쪼갠다 — Supervisor가 상한을 넘는 창을 고르는
        # 일 자체가 없게 만든다.
        candidates.extend(split_span(start, end))

    fresh: list[TimeRange] = []
    for candidate in candidates:
        for piece in subtract_spans(candidate, state.analyzed_windows):
            if piece.end - piece.start >= MIN_USEFUL_WINDOW:
                fresh.append(piece)

    fresh.sort(key=lambda window: window.start, reverse=True)
    if len(fresh) > limit:
        _logger.info("[planner] 후보 %d개 중 %d개만 남긴다", len(fresh), limit)
    return fresh[:limit]


def initial_windows(
    first_seen, last_seen, *, lookback_minutes: int = 5
) -> list[TimeRange]:
    """관측된 유입을 감싸는 첫 분석 구간들.

    앞쪽으로 ``lookback_minutes``만큼 더 본다. 원인은 사고 구간이 아니라 그
    앞에서 만들어지는 경우가 많다 — heap이 서서히 오르거나 샤드 재배치가 앞서
    시작된 경우, 유입 구간만 보면 시작점을 통째로 놓친다.

    확장 비용은 유계다. 조회가 분 단위로 쪼개져 분·소스마다 LIMIT이 걸리므로
    5분은 분 세그먼트 5개가 늘어나는 것에 지나지 않는다. 조용한 구간의
    slowlog는 그 상한에 한참 못 미친다 — slowlog는 임계 초과 쿼리만 기록되므로,
    그 구간이 조용했다는 것이 사고가 아니었다는 뜻이다.

    구간은 분 경계로 내림·올림한다. 운영자와 모델이 모두 분 단위로 읽고,
    조회도 분 단위로 쪼개진다.
    """
    start = (first_seen - timedelta(minutes=lookback_minutes)).replace(
        second=0, microsecond=0
    )
    end = (last_seen + timedelta(minutes=1)).replace(second=0, microsecond=0)
    if end <= start:
        end = start + timedelta(minutes=1)
    return split_span(start, end)
