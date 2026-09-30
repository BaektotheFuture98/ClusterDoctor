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

from cluster_doctor.incident_analysis_agent.model.basemodel.time_range import (
    MAX_TIME_RANGE_DURATION,
    TimeRange,
    merge_spans,
    split_span,
    subtract_spans,
)
from cluster_doctor.incident_orchestrator_agent.model.state.incident_state import IncidentState

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
        # 하므로 여기서 다시 쪼갠다 — Main Agent가 상한을 넘는 창을 고르는
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


def initial_windows(first_seen, last_seen) -> list[TimeRange]:
    """관측된 유입의 시작점 근처로 딱 하나짜리 첫 후보를 만든다.

    원인은 관측 구간 전체가 아니라 최초 발생 시점 근처에 있는 경우가
    많다 — 그래서 여기서는 시작점 하나만 내놓는다. 더 이전을 볼지, 이
    후보 이후를 더 볼지는 코드가 미리 정하지 않는다. 그 판단은
    ``list_candidate_windows``가 보여주는 정보(유입이 멎은 시각 포함)를
    보고 모델이 한다.

    구간은 분 경계로 내림·올림한다. 운영자와 모델이 모두 분 단위로 읽고,
    조회도 분 단위로 쪼개진다.
    """
    start = first_seen.replace(second=0, microsecond=0)
    end = (last_seen + timedelta(minutes=1)).replace(second=0, microsecond=0)
    if end <= start:
        end = start + timedelta(minutes=1)
    end = min(end, start + MAX_TIME_RANGE_DURATION)
    return [TimeRange(start=start, end=end)]
