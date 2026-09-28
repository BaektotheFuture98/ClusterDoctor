"""Application commands passed between inbound use cases."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from cluster_doctor.domain.incident.models import Incident


@dataclass(frozen=True)
class StartIncident:
    """정착된 유입을 Incident 진단으로 넘기는 애플리케이션 명령.

    구간별 위임인 LogAnalysisRequest와 달리 Incident 전체의 시작을 요청한다.
    """

    incident: Incident
    observed_start: datetime
    observed_end: datetime
    settling_wait_seconds: float
