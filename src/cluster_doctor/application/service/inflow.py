"""slowlog 유입이 멎기를 기다리고, 관측된 유입 구간을 만든다.

대기를 런타임 코드가 맡는 이유는 그것이 판단이 아니라 제어이기 때문이다 —
얼마나 기다릴지는 예산 문제이고, 예산을 프롬프트 문장으로 두면 강제되지 않는다.

기다리는 동안 분석은 시작조차 되지 않고 큐만 쌓이므로, 1회를 짧게 끊어 매
사이클 유입을 다시 본다. 한 번에 5분을 자면 그 사이 유입이 멎어도 알아채지
못한다.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from cluster_doctor.domain.model.log_entry import LogEntry

_logger = logging.getLogger(__name__)

# 파이프라인 지연을 의심하는 문턱.
PIPELINE_DELAY_LIMIT = timedelta(minutes=30)

# 유입이 멎었다고 보는 연속 0건 횟수. 1로는 부족하다 — 커넥터 폴링 주기 때문에
# 유입이 계속되는 중에도 한 번은 0건이 나올 수 있다.
SETTLED_ZERO_STREAK = 2


def base_time(log_time: datetime, kafka_receive_time: datetime) -> tuple[datetime, str]:
    """유입 시작 시각의 기준을 정한다.

    돌려주는 문자열은 리포트의 "사용한 시각 기준" 필드로 그대로 간다. 그래야
    그 값이 모델의 주장이 아니라 관측된 사실이 된다.
    """
    if log_time > kafka_receive_time:
        # clock skew. slowlog가 수신보다 미래일 수는 없다.
        return kafka_receive_time, "kafka_receive_time (clock skew)"
    if kafka_receive_time - log_time > PIPELINE_DELAY_LIMIT:
        return kafka_receive_time, "kafka_receive_time (파이프라인 지연 30분 초과)"
    return log_time, "slowlog_timestamp"


@dataclass
class InflowTracker:
    """관측된 유입의 양 끝과 정착 여부.

    ``first_seen``의 초기값이 기준 시각인 이유: 큐에서 꺼낸 것이 하나도 없어도
    분석할 구간은 있어야 한다. 트리거가 걸렸다는 것 자체가 그 시각에 slowlog가
    있었다는 뜻이다.
    """

    first_seen: datetime
    last_seen: datetime
    time_basis: str
    zero_streak: int = 0
    total_wait_seconds: float = 0.0

    @classmethod
    def start(cls, log_time: datetime, kafka_receive_time: datetime) -> "InflowTracker":
        base, basis = base_time(log_time, kafka_receive_time)
        return cls(first_seen=base, last_seen=base, time_basis=basis)

    @property
    def settled(self) -> bool:
        return self.zero_streak >= SETTLED_ZERO_STREAK

    def observe(self, entries: list[LogEntry], *, now: datetime) -> int:
        """큐에서 꺼낸 항목을 반영하고 건수를 돌려준다.

        **미래 시각을 눌러 쓴다.** slowlog가 미래에 발생할 수는 없다. 노드 시계가
        앞서 있으면 timestamp가 지금보다 뒤인 값으로 들어오고, 그대로 쓰면
        ``last_seen``이 미래가 된다. ``first_seen``은 clock skew를 잡아
        수신 시각으로 눌러 둔 값이라 기준이 서로 달라지고, 유입 구간이
        "수신 시각 ~ 미래"로 벌어진다 — 실측에서 13:14~16:16(3시간)이 되어
        커버리지 판정이 "10,200초를 못 봤다"고 말했다.
        """
        if not entries:
            self.zero_streak += 1
            return 0

        times = sorted(min(entry.timestamp, now) for entry in entries)
        ahead = sum(1 for entry in entries if entry.timestamp > now)
        if ahead:
            _logger.warning(
                "[inflow] 발생 시각이 현재보다 미래인 slowlog %d건 — "
                "clock skew로 보고 현재 시각으로 눌러 쓴다",
                ahead,
            )

        self.zero_streak = 0
        # 재트리거로 실행된 경우 기준 시각이 실제 발생보다 늦을 수 있어,
        # 관측된 것이 더 이르면 그쪽으로 당긴다.
        self.first_seen = min(self.first_seen, times[0])
        self.last_seen = max(self.last_seen, times[-1])
        return len(entries)
