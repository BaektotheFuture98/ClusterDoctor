"""Analysis protocol; the implementation returns a framework-independent DTO."""

from typing import Protocol

from cluster_doctor.incident_orchestrator_agent.model.lifecycle import (
    IncidentAnalysisRequest,
    IncidentAnalysisResult,
)


class IncidentAnalyzer(Protocol):
    def analyze(self, request: IncidentAnalysisRequest) -> IncidentAnalysisResult:
        """Run one Main Agent synchronously and return its final projection."""
        ...
