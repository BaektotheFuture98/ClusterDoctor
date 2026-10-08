"""근거 한 건을 사람이 읽을 인용 한 줄로 그린다.

``output_mapping``과 ``incident_timeline`` 두 프로젝션이 근거를 인용할 때
같은 형식을 써야 한다. 따로 구현하면 한쪽만 고쳐지는 사고가 난다 —
``report_text.py`` 모듈 독스트링이 표현 함수를 한 곳에 모아 두는 이유와 같다.
"""

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


def citation_lines(citation: EvidenceCitation) -> list[str]:
    """Shared, lossless citation content for HTML and the plain-text fallback."""
    e = citation.evidence
    if e is None:
        return [f"근거 {citation.evidence_id}: 존재하지 않는 근거 참조"]
    lines = [
        f"근거 [{e.evidence_id}] · {e.source}",
        f"로그 시각: {kst_stamp(e.event_time)}",
    ]
    if e.node_name or e.node_id:
        lines.append(f"대상 노드: {e.node_name or e.node_id}")
    origin = {
        "inherited": "직전 로그에서 상속",
        "fallback": "조회 구간 시작으로 대체",
    }.get(e.time_origin)
    if origin:
        lines.append(f"시각 주의: {origin}")
    p = e.provenance
    if p is None:
        lines.append("수집 위치 미확인")
    else:
        lines.append(
            f"수집 방식: { {'ssh': 'SSH', 'clickhouse': 'ClickHouse', 'elasticsearch_api': 'Elasticsearch API'}[p.method] }"
        )
        if p.host:
            lines.append(
                f"{'접속 호스트' if p.method == 'ssh' else '원본 호스트'}: {p.host}"
            )
        if p.table:
            lines.append(f"조회 DB·테이블: {p.table}")
        if p.file_path:
            lines.append(f"원본 파일: {p.file_path}")
        if p.endpoint:
            lines.append(f"조회 API: {p.endpoint}")
        if p.role == "master":
            lines.append("수집 대상: 수집 당시 마스터로 확인된 노드")
        if p.query_from and p.query_to:
            lines.append(
                f"조회 구간: {kst_stamp(p.query_from)} ~ {kst_stamp(p.query_to)}"
            )
        if p.collected_at:
            lines.append(f"수집 시각: {kst_stamp(p.collected_at)}")
        if p.excerpt:
            lines.append("수집 범위: 필터링된 발췌 (전체 로그가 아님)")
        if not any((p.host, p.table, p.file_path, p.endpoint)):
            lines.append("수집 위치 미확인")
    return lines


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
