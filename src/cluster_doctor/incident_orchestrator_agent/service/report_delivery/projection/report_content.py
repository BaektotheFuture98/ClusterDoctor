"""Shared operator content; internal references stay inside DTOs."""
from dataclasses import replace
from cluster_doctor.incident_analysis_agent.model.evidence import Evidence
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.incident_timeline import project_timeline, TimelineCard, TimelineItem


def stamp(moment):
    return moment.astimezone(KST).isoformat(sep=' ') + ' KST'


def evidence_text(e: Evidence) -> str:
    time = stamp(e.event_time) if e.time_origin == 'parsed' else ('앞선 로그 문맥 · 시각 미확인' if e.time_origin == 'inherited' else '시각 미확인')
    provenance=e.provenance
    location=' · '.join(str(x) for x in (provenance.host,provenance.file_path,provenance.table,provenance.endpoint) if x) if provenance else ''
    return ' · '.join(x for x in (time,str(e.source),e.node_name or e.node_id or '',location,'원문 잘림' if e.raw_truncated else '',e.raw or e.message + ' (원문 없음)') if x)


def visible_citations(citations):
    return tuple(evidence_text(c.evidence) if c.evidence else '인용 원문 확인 불가' for c in citations)


def report_timeline(report):
    cards=list(project_timeline(report.observations,report.evidence,report.timeline_annotations,verification_status=report.verification_status))
    rows=rank_query_requests(report.observations.query_requests)
    required=None
    if rows and rows[0].execution_seconds is not None:
        row=rows[0]
        title=f'최대 실행시간 {row.execution_seconds}s · {row.record.cmd}'
        required=TimelineCard(start=row.record.timestamp,end=row.record.timestamp,severity='Info',representative_event=title,
            impacts=(TimelineItem(' · '.join(row.record.keyword) or '저장된 키워드 없음'),))
        cards=[c for c in cards if not (c.start==required.start and '실행시간' in c.representative_event)]
    rank={'Critical':3,'Warning':2,'Info':1,'':0}
    selected=sorted(cards,key=lambda c:(-rank.get(c.severity,0),c.start))[:7 if required else 8]
    if required:selected.append(required)
    return tuple(sorted(selected,key=lambda c:c.start))
