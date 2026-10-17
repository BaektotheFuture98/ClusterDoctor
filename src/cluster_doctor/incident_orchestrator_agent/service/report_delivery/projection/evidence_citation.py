"""근거 참조를 구조화된 인용으로 연결하고 타임라인용 인용 문자열을 만든다."""

from __future__ import annotations

from datetime import datetime

from cluster_doctor.incident_analysis_agent.model.evidence import Evidence
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_orchestrator_agent.model.evidence_citation import (
    EvidenceCitation,
)


def citations(
    refs: tuple[str, ...], evidence: tuple[Evidence, ...]
) -> tuple[EvidenceCitation, ...]:
    by_id = {item.evidence_id: item for item in evidence}
    return tuple(EvidenceCitation(ref, by_id.get(ref)) for ref in dict.fromkeys(refs))


def kst_stamp(moment: datetime) -> str:
    return moment.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S KST")


def cite(evidence: Evidence) -> str:
    """근거 하나를 사람이 읽을 한 줄로 그린다."""
    parts = [
        f"[{evidence.evidence_id}]",
        kst_stamp(evidence.event_time),
    ]
    parts.append(str(evidence.source))
    if evidence.severity:
        parts.append(evidence.severity)
    if evidence.node_name or evidence.node_id:
        parts.append(f"node={evidence.node_name or evidence.node_id}")
    parts.append(evidence.message)
    return " | ".join(parts)
