"""해소된 참조는 식별 정보와 수집 메타데이터를 모두 유지한다."""

from dataclasses import dataclass

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence


@dataclass(frozen=True)
class EvidenceCitation:
    evidence_id: str
    evidence: Evidence | None = None
