"""장애 한 건. Supervisor 실행의 단위.

trigger 하나가 곧 Incident 하나는 아니다. slowlog는 몰려서 오므로 마이크로
배치가 여러 건을 하나로 묶고, 그 묶음이 Incident가 된다.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class IncidentStatus(StrEnum):
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
    SLOWLOG = "SLOWLOG"
    MANUAL = "MANUAL"


class Incident(BaseModel):
    """무엇 때문에 이 분석이 시작됐는가.

    ``trigger_time``과 ``kafka_receive_time``을 둘 다 든다. 둘이 어긋나는 것이
    그 자체로 관측이기 때문이다 — slowlog가 수신보다 미래면 clock skew이고,
    30분 넘게 늦으면 파이프라인 지연이다. 어느 쪽을 기준으로 삼았는지는
    ``time_basis``로 리포트에 실린다.
    """

    model_config = ConfigDict(frozen=True)

    incident_id: str
    cluster: str
    trigger_time: datetime
    kafka_receive_time: datetime
    trigger_type: TriggerType = TriggerType.SLOWLOG
    trigger_metadata: dict[str, str] = Field(default_factory=dict)
