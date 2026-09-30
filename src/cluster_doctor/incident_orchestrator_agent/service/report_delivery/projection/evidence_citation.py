"""근거 한 건을 사람이 읽을 인용 한 줄로 그린다.

``output_mapping``과 ``incident_timeline`` 두 프로젝션이 근거를 인용할 때
같은 형식을 써야 한다. 따로 구현하면 한쪽만 고쳐지는 사고가 난다 —
``report_text.py`` 모듈 독스트링이 표현 함수를 한 곳에 모아 두는 이유와 같다.
"""

from __future__ import annotations

from cluster_doctor.incident_analysis_agent.model.basemodel.evidence import Evidence


def cite(evidence: Evidence) -> str:
    """근거 하나를 사람이 읽을 한 줄로 그린다."""
    parts = [
        f"[{evidence.evidence_id}]",
        evidence.event_time.strftime("%Y-%m-%d %H:%M:%S"),
    ]
    parts.append(str(evidence.source))
    if evidence.severity:
        parts.append(evidence.severity)
    if evidence.node_name or evidence.node_id:
        parts.append(f"node={evidence.node_name or evidence.node_id}")
    parts.append(evidence.message)
    return " | ".join(parts)
