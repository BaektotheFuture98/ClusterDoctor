"""분석 protocol. 구현체는 프레임워크와 무관한 DTO를 반환한다."""

from typing import Protocol

from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentAnalysisRequest,
    IncidentAnalysisResult,
)


class IncidentAnalyzer(Protocol):
    def analyze(self, request: IncidentAnalysisRequest) -> IncidentAnalysisResult:
        """Main Agent 하나를 동기로 실행하고 최종 projection을 반환한다."""
        ...
