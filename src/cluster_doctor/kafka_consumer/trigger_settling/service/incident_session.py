"""incident 범위를 언제 마감하고 세션을 이어 갈지 정하는 규칙.

대기와 마감·계속 판단을 프롬프트가 아니라 코드가 맡는다. 얼마나 기다릴지는
예산이고, "큐가 비었고 green이면 끝"은 판단이 아니라 규칙이다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from cluster_doctor.incident_orchestrator_agent.model.lifecycle import IncidentClose

# incident 하나의 범위 상한. 시작은 최초 관측(세션 첫 incident) 또는 이전
# incident의 끝이다. 긴 미복구를 이 단위로 나눠 보고서가 무한정 늦지 않게 한다.
INCIDENT_SPAN_LIMIT = timedelta(minutes=20)

# 세션 하나가 만드는 incident 수. 클러스터가 몇 시간째 green이 아니어도 분석
# 비용이 이 배수로 묶인다.
MAX_INCIDENTS_PER_SESSION = 3


@dataclass(frozen=True)
class Closing:
    end: datetime
    close: IncidentClose


def decide_close(
    *,
    start: datetime,
    now: datetime,
    arrived: bool,
    green: bool | None,
    limit: timedelta = INCIDENT_SPAN_LIMIT,
) -> Closing | None:
    """이번 확인에서 incident를 마감할지. ``green``이 None이면 health 조회 실패다.

    새 신호가 있으면 health를 보지 않는다 — 유입이 이어지는 동안은 green이어도
    끝난 것이 아니다. 끝은 상한을 넘지 않는다. 분석이 상한보다 오래 걸려 다음
    incident의 첫 확인이 이미 상한 뒤라면 [시작, 시작+상한]으로 바로 마감된다.
    """
    cap = start + limit
    if not arrived and green is None:
        return Closing(min(now, cap), IncidentClose.HEALTH_UNKNOWN)
    if not arrived and green:
        return Closing(min(now, cap), IncidentClose.GREEN)
    if now >= cap:
        return Closing(cap, IncidentClose.SPAN_LIMIT)
    return None


def continue_session(*, sequence: int, queued: bool, green: bool | None) -> bool:
    """발행 뒤 다음 incident를 만들지.

    큐에 신호가 있으면 상태와 상관없이 잇는다. 큐가 비었으면 green이 아닐 때만
    잇는다. 조회 실패로 끝내는 이유: 상태를 모르는 채 이으면 세션 상한까지 빈
    incident를 분석한다.
    """
    if sequence >= MAX_INCIDENTS_PER_SESSION:
        return False
    if queued:
        return True
    return green is False
