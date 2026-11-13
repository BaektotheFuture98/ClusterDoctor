from dataclasses import dataclass, replace
from datetime import datetime


@dataclass(frozen=True)
class HealthPoint:
    """클러스터 상태가 유지된 한 구간.

    health는 incident 구성 중에는 30초마다, 분석 구간마다 한 번씩 조회된다.
    호출마다 한 줄을 쌓으면 같은 green이 열 줄 늘어서고, 그 목록은 "상태 변화를
    시간순으로"라는 리포트의 요구를 오히려 가린다. 그래서 직전과 같은 상태면 새
    항목을 만들지 않고 ``until``만 늘린다(``fold_health``).
    """

    at: datetime
    until: datetime
    status: str
    unassigned_shards: int = 0
    active_shards: int = 0
    number_of_nodes: int = 0


def fold_health(
    points: tuple[HealthPoint, ...], payload: dict, now: datetime
) -> tuple[HealthPoint, ...]:
    """health 응답 하나를 이력에 접는다.

    수집기(분석 시점)와 incident 구성(사건 당시)이 같은 규칙을 쓴다. payload가
    dict가 아니거나 숫자 칸에 숫자가 아닌 값이 오면 예외가 난다. 호출자가 조회
    실패로 다룬다.
    """
    status = str(payload.get("status", ""))
    unassigned = int(payload.get("unassigned_shards", 0) or 0)
    if points and points[-1].status == status and points[-1].unassigned_shards == unassigned:
        return (*points[:-1], replace(points[-1], until=now))
    return (
        *points,
        HealthPoint(
            at=now,
            until=now,
            status=status,
            unassigned_shards=unassigned,
            active_shards=int(payload.get("active_shards", 0) or 0),
            number_of_nodes=int(payload.get("number_of_nodes", 0) or 0),
        ),
    )


def is_green(payload: dict) -> bool:
    return str(payload.get("status", "")).lower() == "green"
