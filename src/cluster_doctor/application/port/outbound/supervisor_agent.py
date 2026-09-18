"""Supervisor 판단 포트.

한 사이클에 한 번 불려 ``SupervisorDecision`` 하나를 돌려준다. 루프도,
Guardrail도, State 갱신도 여기 없다 — 그것은 ``IncidentOrchestrator``의 몫이다.
모델에게 루프를 맡기면 상한이 프롬프트 문장이 되고, 프롬프트 문장은 강제가
아니다.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from cluster_doctor.domain.model.incident import Incident
from cluster_doctor.domain.model.incident_state import IncidentState
from cluster_doctor.domain.model.log_analysis import LogAnalysisResponse
from cluster_doctor.domain.model.supervisor_decision import SupervisorDecision
from cluster_doctor.domain.model.time_range import TimeRange


@runtime_checkable
class SupervisorAgent(Protocol):
    def decide(
        self,
        incident: Incident,
        state: IncidentState,
        *,
        last_response: LogAnalysisResponse | None = None,
        candidate_windows: tuple[TimeRange, ...] = (),
    ) -> SupervisorDecision:
        """다음 행동 하나를 고른다.

        ``candidate_windows``는 코드가 미리 계산한 "아직 보지 않은 구간"이다.
        차집합 산수를 모델에게 시키지 않는 이유는 그것이 판단이 아니라 계산이고,
        계산을 모델이 하면 틀리기 때문이다. 모델이 하는 일은 **그중 무엇을 왜
        볼 것인가**, 혹은 더 볼 필요가 없다는 판단이다.

        실패해도 예외를 올리지 않는다. 판단할 수 없으면 종료를 고른다 —
        모델이 죽었다고 리포트까지 잃을 이유가 없다.
        """
        ...
