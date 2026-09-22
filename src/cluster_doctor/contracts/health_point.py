from dataclasses import dataclass
from datetime import datetime


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
