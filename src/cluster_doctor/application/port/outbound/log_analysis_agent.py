"""Log Analysis SubAgent 포트.

Supervisor가 아는 것은 이 함수 하나뿐이다. datasource도, LangGraph도, SSH도
이 경계 뒤에 있다 — 그것이 "Main Agent가 HOW를 제어하지 않는다"의 코드 표현이다.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from cluster_doctor.domain.model.log_analysis import (
    LogAnalysisRequest,
    LogAnalysisResponse,
)


@runtime_checkable
class LogAnalysisAgent(Protocol):
    def analyze(self, request: LogAnalysisRequest) -> LogAnalysisResponse:
        """한 analysis window를 조사하고 구조화된 결과를 돌려준다.

        **예외를 올리지 않는다.** 실패는 ``status=FAILED``로 표현한다. 예외로
        올리면 Supervisor 사이클이 죽고, 그때까지 모은 Evidence와 리포트가 함께
        사라진다 — 이 저장소가 "리포트는 항상 전달된다"로 지켜 온 것이 그것이다.

        요청 범위 밖의 시간이 필요하면 ``NEED_MORE_CONTEXT``와
        ``suggested_windows``를 돌려준다. 스스로 범위를 넓히지 않는다.
        """
        ...
