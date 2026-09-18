"""IncidentState 보관 포트.

첫 구현은 in-memory이고 그것으로 충분하다. 포트를 두는 이유는 교체 가능성이
실재하기 때문이다 — 프로세스가 재시작되면 진행 중이던 Incident가 사라지는데,
그것을 고치려면 Redis나 PostgreSQL 구현이 필요하고 그때 application 코드가
바뀌어서는 안 된다.

``Protocol``인 것은 구현체가 이 모듈을 import하지 않아도 되게 하려는 것이다.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from cluster_doctor.domain.model.incident_state import IncidentState


@runtime_checkable
class IncidentStateRepository(Protocol):
    def create(self, state: IncidentState) -> None:
        """새 Incident의 상태를 만든다. 이미 있으면 예외를 올린다.

        ``save``와 나누는 이유는 덮어쓰기와 새로 만들기가 다른 사고이기
        때문이다. 같은 incident_id로 두 번 시작하는 것은 버그이고, 그것이
        조용히 앞선 상태를 지우면 진행 중이던 분석을 잃는다.
        """
        ...

    def get(self, incident_id: str) -> IncidentState:
        """없으면 ``IncidentNotFoundError``."""
        ...

    def save(self, state: IncidentState) -> None:
        """기존 상태를 갱신한다."""
        ...
