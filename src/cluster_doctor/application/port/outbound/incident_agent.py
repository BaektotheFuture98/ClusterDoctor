"""Incident 하나의 분석 진행을 결정하는 Agent의 경계.

이 포트가 존재하는 이유는 하나다: **application 계층이 ``deepagents``나
``langchain``을 몰라야 한다.** 구현은 Main DeepAgent이고 내부에서 tool loop를
돌지만, 그 사실은 인프라 쪽 이야기다. 이쪽이 아는 것은 "Incident 하나를 맡기면
어떻게 끝났는지 돌려준다"뿐이다.

앞선 구조에서는 ``SupervisorAgent.decide``가 판단 하나를 돌려주고 루프는
``IncidentOrchestrator``가 돌았다. 지금은 루프가 Agent 안으로 들어갔다 —
대신 상한은 따라 들어가지 않았다. 예산·중복·위임 횟수는 여전히 코드가 쥐고
있고(구현 쪽 미들웨어), 모델이 넘길 수 있는 것은 "무엇을 볼 것인가"와
"이제 충분한가"뿐이다. 그 경계가 무너지면 상한은 프롬프트 문장이 된다.

**구현은 예외를 올리지 않는다.** 리포트는 항상 전달되어야 하고, 여기서 예외가
새면 코드가 모아 둔 관측값까지 함께 사라진다.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from cluster_doctor.domain.model.incident import Incident, IncidentStatus
from cluster_doctor.domain.model.incident_state import IncidentState


@dataclass(frozen=True)
class IncidentAgentResult:
    """Agent가 Incident를 어떻게 끝냈는가.

    분석 결과 자체는 여기 없다. ``IncidentState``와 ``ArtifactStore``가 이미
    들고 있고, 같은 것을 두 곳에 두면 한쪽만 갱신되는 순간 조용히 갈린다.
    """

    status: IncidentStatus
    reason: str = ""
    # 분석이 실패했는가. ``gaps``(근거 일부 누락)와 다르다 — 이쪽만 재트리거를
    # 막는다.
    failed: bool = False
    gaps: tuple[str, ...] = ()


class IncidentAgent(Protocol):
    """Incident 하나의 분석을 끝까지 진행한다."""

    def run(self, incident: Incident, state: IncidentState) -> IncidentAgentResult:
        """분석을 진행하고 종료 상태를 돌려준다.

        ``state``를 제자리에서 갱신하고 저장소에도 반영한다 — 예산 회계가
        Agent 안에서 일어나기 때문이다. 호출부는 돌아온 뒤 ``state``를 다시
        읽어야 한다.

        **동기 호출이고 수 분 걸릴 수 있다.** 같은 이벤트 루프가 Kafka를
        소비하므로 호출부가 별도 스레드로 밀어야 한다.
        """
        ...
