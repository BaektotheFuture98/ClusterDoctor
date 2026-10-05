"""장애 한 건. Main Agent 실행의 단위.

신호 하나가 곧 Incident 하나는 아니다. 문제성 로그 신호는 몰려서 오므로 마이크로
배치가 여러 건을 하나로 묶고, 그 묶음이 Incident가 된다(``kafka_consumer``의
trigger_settling 서비스가 그 묶음을 만든다).
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class IncidentStatus(StrEnum):
    """Incident 생명주기의 업무 상태.

    구간 분석 결과와 별개이며 종료 상태는 완료·실패·취소 세 가지다.
    """

    OPEN = "OPEN"
    ANALYZING = "ANALYZING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    def is_terminal(self) -> bool:
        return self in (
            IncidentStatus.COMPLETED,
            IncidentStatus.FAILED,
            IncidentStatus.CANCELLED,
        )


class TriggerType(StrEnum):
    """Incident를 시작한 유입의 종류.

    문제성 로그 자동 유입과 수동 요청의 시작 맥락을 구별한다.
    """

    PROBLEM_LOG = "PROBLEM_LOG"
    MANUAL = "MANUAL"


class Incident(BaseModel):
    """무엇 때문에 이 분석이 시작됐는가.

    ``trigger_time``과 ``kafka_receive_time``을 둘 다 든다. 둘이 어긋나는 것이
    그 자체로 관측이기 때문이다 — 로그 발생 시각이 수신 시각보다 미래면 clock skew이고,
    30분 넘게 늦으면 파이프라인 지연이다. 어느 쪽을 기준으로 삼았는지는
    ``time_basis``로 리포트에 실린다.
    """

    model_config = ConfigDict(frozen=True)

    incident_id: str
    cluster: str
    trigger_time: datetime
    kafka_receive_time: datetime
    trigger_type: TriggerType = TriggerType.PROBLEM_LOG
    trigger_metadata: dict[str, str] = Field(default_factory=dict)
