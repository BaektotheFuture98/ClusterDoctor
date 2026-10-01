"""A resolved reference retains both identity and collection metadata."""

from dataclasses import dataclass

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence


@dataclass(frozen=True)
class EvidenceCitation:
    evidence_id: str
    evidence: Evidence | None = None
