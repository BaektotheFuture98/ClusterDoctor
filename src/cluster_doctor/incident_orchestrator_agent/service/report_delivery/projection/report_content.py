"""Shared operator content; internal references stay inside DTOs."""
from dataclasses import replace
import re
from cluster_doctor.incident_analysis_agent.model.evidence import Evidence
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.incident_timeline import project_timeline, TimelineCard, TimelineItem


def stamp(moment):
    return moment.astimezone(KST).isoformat(sep=' ') + ' KST'


def minute_stamp(moment):
    return moment.astimezone(KST).strftime('%Y-%m-%d %H:%M') + ' KST'


def evidence_text(e: Evidence) -> str:
    time = stamp(e.event_time) if e.time_origin == 'parsed' else ('앞선 로그 문맥 · 시각 미확인' if e.time_origin == 'inherited' else '시각 미확인')
    provenance=e.provenance
    location=' · '.join(str(x) for x in (provenance.host,provenance.file_path,provenance.table,provenance.endpoint) if x) if provenance else ''
    return ' · '.join(x for x in (time,str(e.source),e.node_name or e.node_id or '',location,e.message) if x)


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
    def concise(text):
        return re.sub(r"\s*\([^)]*중앙값[^)]*\)", "", text).strip()
    visible=[]
    for card in sorted(selected,key=lambda c:c.start):
        title=concise(card.representative_event)
        seen={title}
        groups=[]
        for items in (card.impacts,card.causes):
            group=[]
            for item in items:
                text=concise(item.text)
                if text and text not in seen:
                    group.append(replace(item,text=text));seen.add(text)
            groups.append(tuple(group))
        visible.append(replace(card,representative_event=title,impacts=groups[0],causes=groups[1]))
    return tuple(visible)


def system_maxima(nodes):
    measured=tuple(node for node in nodes if node.samples > 0)
    result=[]
    for label,field,unit in (('CPU','cpu_max','%'),('JVM heap','jvm_heap_max','%'),
            ('Search 큐','search_queue_max',''),('Write 큐','write_queue_max','')):
        if not measured:break
        maximum=max(getattr(node,field) for node in measured)
        names=(f'전체 관측 노드 {len(measured)}개' if field in ('search_queue_max','write_queue_max') and maximum==0 else ', '.join(node.node for node in measured if getattr(node,field)==maximum))
        result.append(f'{label} 최대 {maximum}{unit} · {names}')
    return tuple(result)


def timeline_sources(card, shown_evidence=()):
    """Sources omitted from standalone tables remain visible for displayed cards."""
    shown={e.evidence_id for e in shown_evidence}
    return tuple(c for c in card.evidence_citations if c.evidence_id not in shown)


def execution_summary(observations):
    """Observed facts only; never infer keyword or node causation."""
    rows=rank_query_requests(observations.query_requests)
    if not rows:
        return ('수집된 쿼리 실행 로그가 없습니다.',)
    valid=next((row for row in rows if row.execution_seconds is not None),None)
    if valid is None:
        return ('수집 로그에서 실행시간을 확인할 수 없습니다.',)
    record=valid.record
    lines=[f'관측된 실행 로그에서 {record.cmd}의 최대 실행시간은 {valid.execution_seconds}초입니다.',
           f'로그 시각: {stamp(record.timestamp)}',
           '저장된 키워드: '+(' · '.join(record.keyword) or '없음')]
    if record.keyword_omitted:
        lines[-1]+=f' (+{record.keyword_omitted}개 생략)'
    lines.append(f'대상: {valid.target_host or "미확인"}')
    if record.date_range:
        lines.append(f'조회 기간: {record.date_range}일 · {record.s_date} ~ {record.e_date}')
    return tuple(lines)


def display_narrative(report, *, analysis_failed=False):
    """Use the standard narrative slots even when analysis is unavailable."""
    from cluster_doctor.incident_orchestrator_agent.model.incident_report import Narrative, CauseAssessment
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.evidence_citation import citations
    if report.verification_status=='PASSED' and report.narrative and not analysis_failed:
        return report.narrative
    rows=rank_query_requests(report.observations.query_requests)
    refs=()
    if rows:
        refs=tuple(e.evidence_id for e in report.evidence if e.record_key==rows[0].record_key)
    source=citations(refs,report.evidence)
    return Narrative(headline=' '.join(execution_summary(report.observations)),headline_citations=source,
        causes=(CauseAssessment(statement='수집된 자료로 지연 원인을 확정할 수 없습니다.',confidence='미확인',supporting=source),))


MASTER_DISPLAY_LIMIT = 120
SOURCE_DISPLAY_LIMIT = 10


def source_log_sections(report):
    """Fixed source slots, with collection observations authoritative for master logs."""
    master=tuple(' · '.join(x for x in (
        stamp(e.timestamp) if e.timestamp else '시각 미확인', e.node, e.level, e.logger, e.line or e.rendered) if x)
        for e in report.observations.master_events[:MASTER_DISPLAY_LIMIT])
    slow=tuple(evidence_text(e) for e in sorted((e for e in report.evidence if e.source=='slowlog'),key=lambda e:e.event_time)[:SOURCE_DISPLAY_LIMIT])
    return (('master-logs','마스터 노드 로그',master),('slowlogs','slowlog 로그',slow))


def is_validation_diagnostic(text):
    return text.startswith(('리포트 검증 불일치:', '리포트 검증이 실패했다:'))
