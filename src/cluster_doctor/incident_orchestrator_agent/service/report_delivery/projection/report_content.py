"""운영자에게 공통으로 보이는 내용. 내부 참조는 DTO 안에만 둔다."""
from dataclasses import replace
from collections.abc import Iterable
import re
from cluster_doctor.incident_analysis_agent.model.evidence import Evidence
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.incident_timeline import project_timeline, TimelineCard, TimelineItem
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.source_status import master_note, slowlog_note
from cluster_doctor.incident_orchestrator_agent.model.evidence_citation import EvidenceCitation
from cluster_doctor.incident_orchestrator_agent.model.incident_report import CauseAssessment, IncidentAnalysisReport, Narrative


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


def unique_citations(*groups: Iterable[EvidenceCitation]) -> tuple[EvidenceCitation, ...]:
    """표시할 인용들을 근거 ID로 중복 제거하며 처음 나온 인용을 유지한다."""
    by_id = {}
    for group in groups:
        for citation in group:
            by_id.setdefault(citation.evidence_id, citation)
    return tuple(by_id.values())


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
    """별도 원문 영역에 표시하지 않은 타임라인 근거 인용을 고른다."""
    shown={e.evidence_id for e in shown_evidence}
    return tuple(c for c in card.evidence_citations if c.evidence_id not in shown)


def display_narrative(report: IncidentAnalysisReport, *, analysis_failed: bool = False) -> Narrative:
    """검증 통과·분석 성공 시 원래 판단을, 그 외에는 관측 기반 대체 판단을 반환한다."""
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.evidence_citation import citations
    if report.verification_status=='PASSED' and report.narrative and not analysis_failed:
        return report.narrative
    rows=rank_query_requests(report.observations.query_requests)
    refs=()
    if rows:
        refs=tuple(e.evidence_id for e in report.evidence if e.record_key==rows[0].record_key)
    source=citations(refs,report.evidence)
    return Narrative(causes=(CauseAssessment(statement='수집된 자료로 지연 원인을 확정할 수 없습니다.',supporting=source),))


MASTER_DISPLAY_LIMIT = 120
SOURCE_DISPLAY_LIMIT = 10


def shown_slowlog_evidence(report):
    return tuple(sorted((e for e in report.evidence if e.source=='slowlog'),key=lambda e:e.event_time)[:SOURCE_DISPLAY_LIMIT])


def slowlog_nodes(report):
    """slowlog 근거에 나온 노드 이름. 처음 나온 순서를 유지한다."""
    return tuple(dict.fromkeys(e.node_name for e in report.evidence if e.source=='slowlog' and e.node_name))


def metric_nodes(report, limit=10):
    """slowlog에 나온 노드를 먼저 두고 나머지는 큐·CPU 최대값 순. 지표 행이 없는 노드는 만들지 않는다."""
    nodes=tuple(n for n in report.observations.nodes if n.samples > 0)
    slow=set(slowlog_nodes(report))
    return tuple(sorted(nodes,key=lambda r:(r.node not in slow,-max(r.search_queue_max,r.write_queue_max),-r.cpu_max,r.node))[:limit])


def _strip_brackets(text):
    """중첩 포함 대괄호 내용을 []로 접는다."""
    out=[];depth=0
    for ch in text:
        if ch=='[':
            if depth==0:out.append('[]')
            depth+=1
        elif ch==']':
            depth=max(depth-1,0)
        elif depth==0:
            out.append(ch)
    return ''.join(out)


def _master_key(e):
    text=_strip_brackets(e.line or e.rendered or '')
    text=re.sub(r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?','',text)
    return (e.level.strip().upper(),e.logger,re.sub(r'\d+','',text).strip())


def _master_groups(events):
    """같은 모양의 master 로그를 한 묶음으로 모은다. ERROR, WARN, 그 외 순이며 같은 레벨 안에서는 첫 시각순."""
    groups={}
    for e in events:
        groups.setdefault(_master_key(e),[]).append(e)
    rank=lambda g:0 if g[0].level.strip().upper()=='ERROR' else 1 if g[0].level.strip().upper() in ('WARN','WARNING') else 2
    far=max((e.timestamp for e in events if e.timestamp),default=None)
    return sorted(groups.values(),key=lambda g:(rank(g),g[0].timestamp is None,g[0].timestamp or far))


def _master_group_line(group):
    e=group[0]
    line=' · '.join(x for x in (stamp(e.timestamp) if e.timestamp else '시각 미확인',e.node,e.level,e.logger,e.line or e.rendered) if x)
    if len(group)>1:
        times=[x.timestamp for x in group if x.timestamp]
        span=f' ({min(times).astimezone(KST):%H:%M:%S}~{max(times).astimezone(KST):%H:%M:%S})' if times else ''
        line+=f' · 외 {len(group)-1}건{span}'
    return line


def source_log_sections(report, marks=None):
    """source slot은 고정이며, master log는 수집 관측값이 우선한다.

    marks는 evidence_id별 표지로, 서술이 인용한 slowlog 줄 앞에 붙여 근거 섹션에서 같은 줄이 두 번 나오지 않게 한다."""
    marks=marks or {}
    master=tuple(_master_group_line(g) for g in _master_groups(report.observations.master_events)[:MASTER_DISPLAY_LIMIT])
    slow=tuple((f'[{marks[e.evidence_id]}] ' if e.evidence_id in marks else '')+evidence_text(e) for e in shown_slowlog_evidence(report))
    return (('slowlogs','slowlog 로그',slow),('master-logs','마스터 노드 로그',master))


def source_log_notes(report):
    """section id별 빈 섹션 설명. 항목이 없는 섹션에만 쓴다."""
    sections={id_:lines for id_,_,lines in source_log_sections(report)}
    notes={}
    if not sections['master-logs']:notes['master-logs']=master_note(report.observations)
    if not sections['slowlogs']:notes['slowlogs']=slowlog_note(report.observations)
    return notes


def is_validation_diagnostic(text):
    return text.startswith(('리포트 검증 불일치:', '리포트 검증이 실패했다:'))
