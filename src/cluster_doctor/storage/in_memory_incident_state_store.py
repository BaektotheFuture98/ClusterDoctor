"""IncidentState의 프로세스 내 보관.

프로세스가 죽으면 진행 중이던 Incident도 사라진다. 그것이 이 구현의 한계이고,
포트를 둔 이유다 — Redis 구현으로 갈아끼울 때 application 코드는 바뀌지 않는다.

``RLock``으로 감싸는 이유: 트리거 서비스가 ``asyncio.to_thread``로 분석을 띄우고,
유입이 몰리면 여러 Incident가 겹쳐 돈다. dict 갱신 자체는 GIL이 지켜 주지만
read-modify-write 사이에 다른 스레드가 끼어들 수 있다.

**복사해서 넣고 복사해서 준다.** 같은 객체를 돌려주면 호출자가 저장소를 거치지
않고 상태를 바꿀 수 있고, 그러면 ``save``를 부르지 않은 변경이 조용히 반영된다 —
Redis 구현으로 바꾸는 날 그 코드가 전부 깨진다.
"""

from __future__ import annotations

import logging
import threading

from cluster_doctor.exceptions import IncidentNotFoundError
from cluster_doctor.incident.state import IncidentState

_logger = logging.getLogger(__name__)


class InMemoryIncidentStateRepository:
    def __init__(self) -> None:
        self._states: dict[str, IncidentState] = {}
        self._lock = threading.RLock()

    def create(self, state: IncidentState) -> None:
        with self._lock:
            if state.incident_id in self._states:
                raise ValueError(f"이미 존재하는 incident_id: {state.incident_id}")
            self._states[state.incident_id] = state.model_copy(deep=True)
        _logger.info("[state] incident %s 생성", state.incident_id)

    def get(self, incident_id: str) -> IncidentState:
        with self._lock:
            state = self._states.get(incident_id)
        if state is None:
            raise IncidentNotFoundError(f"알 수 없는 incident_id: {incident_id}")
        return state.model_copy(deep=True)

    def save(self, state: IncidentState) -> None:
        with self._lock:
            if state.incident_id not in self._states:
                raise IncidentNotFoundError(
                    f"알 수 없는 incident_id: {state.incident_id}"
                )
            self._states[state.incident_id] = state.model_copy(deep=True)

    def discard(self, incident_id: str) -> None:
        """끝난 Incident를 지운다.

        in-memory 구현에만 있는 정리 수단이다. 포트에 두지 않은 것은 의도적이다 —
        보관 기간 정책은 저장소마다 다르고(Redis는 TTL, RDB는 배치), application이
        그것을 지시하기 시작하면 구현을 갈아끼울 수 없게 된다.
        """
        with self._lock:
            self._states.pop(incident_id, None)
