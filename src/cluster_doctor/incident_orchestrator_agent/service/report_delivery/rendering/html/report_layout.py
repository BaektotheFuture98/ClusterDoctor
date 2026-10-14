"""코드가 만든 사실과 검증된 해석으로 구성한 독립 실행형 리포트.

본문은 결론(판정·원인·조치)·지표·추이·느린 요청만 둔다. 소스 간 연결, 원문 로그,
인용 근거는 "근거 보기" 안에 접는다. 결론을 의심할 때 펼쳐 보는 자료다.
"""
from datetime import datetime
from cluster_doctor.incident_analysis_agent.service.observation.correlation import build_cross_links
from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests
from cluster_doctor.incident_orchestrator_agent.model.incident_report import IncidentAnalysisReport
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import stamp, minute_stamp, report_timeline, visible_citations, system_maxima, timeline_sources, display_narrative, source_log_sections, source_log_notes, shown_slowlog_evidence, metric_nodes, is_validation_diagnostic, unique_citations
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.query_trend import project_query_trend
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.source_status import master_note, query_log_note, summarize_source
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.conclusion import render_conclusion, render_metric_cards
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.cross_source import render_cross_source
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.evidence_link import esc
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.query_ranking import render_query_ranking
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.query_trend import render_query_trend
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.ssh_log_view import render_ssh_logs, ssh_evidence

# 접힌 근거는 인쇄 시 CSS만으로 열리지 않는 브라우저가 있어 인쇄 직전에 펼친다. 고정 문자열이며 입력을 담지 않는다.
PRINT_OPEN_SCRIPT = '<script>addEventListener("beforeprint",()=>document.querySelectorAll("details").forEach(d=>d.open=true))</script>'
DEMO_GAP = "디자인 미리보기용 가상 데이터입니다. 실제 장애 분석 결과가 아닙니다."

_EXTRA_CSS='''
body{background:#f2f5fa;color:#192b43;font-family:system-ui,sans-serif}.wrap{max-width:1180px;margin:auto;padding:28px}
section{background:white;border:1px solid #dde4ef;border-radius:12px;padding:24px;margin:20px 0}
h1{font-size:28px}h2{font-size:21px}
.verdict{font-size:16px;font-weight:600;margin:0 0 8px}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;background:none;border:none;padding:0}
.card{background:white;border:1px solid #dde4ef;border-radius:10px;padding:14px 16px;color:#52657b;font-size:13px}.card strong{display:block;font-size:26px;color:#1555aa;margin-top:4px}
.actions{margin:0;padding-left:20px}.actions li{margin:4px 0}
details#evidence{margin:28px 0}details#evidence>summary{cursor:pointer;font-size:19px;font-weight:700;padding:14px 18px;background:white;border:1px solid #dde4ef;border-radius:12px}
table{width:100%;border-collapse:collapse;table-layout:fixed;font-size:13px}th,td{padding:10px;border-bottom:1px solid #dde4ef;text-align:left;vertical-align:top;overflow-wrap:anywhere;white-space:pre-wrap}th{background:#eef3fb}
pre{white-space:pre-wrap;overflow-wrap:anywhere;font:12px/1.65 ui-monospace,monospace;margin:0}svg{width:100%;height:auto}svg text{font-size:12px;fill:#35506b}.hint{color:#52657b}
.timeline-event h3{margin:0 0 8px}.quote{background:#f5f7fb;padding:12px;border-left:3px solid #adc8ee;margin:10px 0}
@media(max-width:700px){.wrap{padding:12px}section{padding:14px}table{font-size:11px}}
@media print{body{background:white}.wrap{max-width:none;padding:0}section{border-radius:0;box-shadow:none}svg{display:block!important}thead{display:table-header-group}tr,.quote{break-inside:avoid}pre{overflow:visible}a{text-decoration:none}details#evidence>*{display:block}details::details-content{content-visibility:visible;display:block}}
'''


def quotes(items):
    return ''.join(f'<pre class="quote">{esc(text)}</pre>' for text in visible_citations(items))


def render_paragraphs(text: str, *, css_class: str = '') -> str:
    """작성된 문단을 주장이나 마크업으로 해석하지 않고 그대로 표시한다."""
    attribute = f' class="{esc(css_class)}"' if css_class else ''
    return ''.join(f'<p{attribute}>{esc(paragraph.strip())}</p>'
                   for paragraph in text.replace('\r\n', '\n').split('\n\n') if paragraph.strip())


def _notices(obs, gaps) -> str:
    out=''
    for status in (s for s in obs.source_statuses if s.status=='failed'):
        out+=f'<p class="hint">{esc(status.source)} {esc(status.host)} 수집 실패 · {esc(stamp(status.start))}</p>'
    for gap in dict.fromkeys(gaps):
        if gap != DEMO_GAP and not is_validation_diagnostic(gap):out+=f'<p class="hint">{esc(gap)}</p>'
    if any(row.failed for row in obs.timeline):out+='<p class="hint">분석하지 못한 구간이 있습니다.</p>'
    return out


def _timeline(report, narrative) -> str:
    out='<section id="timeline"><h2>주요 타임라인</h2><div class="incident-timeline">'
    for card in report_timeline(report):
        severity_class={'Critical':' timeline-event-critical','Warning':' timeline-event-warning'}.get(card.severity,'')
        out+=f'<article class="timeline-event{severity_class}"><div class="event-time"><time datetime="{esc(card.start.isoformat())}">{esc(minute_stamp(card.start))}</time>'+(f'<time datetime="{esc(card.end.isoformat())}">마지막 관측 {esc(minute_stamp(card.end))}</time>' if card.end!=card.start else '')+'</div><div>'+f'<h3>{esc(card.representative_event)}</h3>'
        for item in (*card.impacts,*card.causes):out+=f'<p>{esc(item.text)}</p>'
        if narrative:
            for item in card.interpretations:out+=f'<p class="analysis-note">{esc(item.text)}</p>'
        out+=quotes(timeline_sources(card,ssh_evidence(report.evidence)))
        out+='</div></article>'
    return out+'</div></section>'


def _metrics(report) -> str:
    obs=report.observations
    out='<section id="system-metrics"><h2>시스템 지표 · 관측 최대값</h2>'
    nodes=tuple(n for n in obs.nodes if n.samples > 0)
    if not nodes:
        return out+'<p>수집된 시스템 지표 없음</p></section>'
    out+=''.join(f'<p>{esc(value)}</p>' for value in system_maxima(nodes))
    out+='<table><thead><tr><th>ES 노드</th><th>CPU(%)</th><th>JVM heap(%)</th><th>Search 큐</th><th>Write 큐</th><th>Search rejected 누적</th><th>Write rejected 누적</th></tr></thead><tbody>'
    for row in metric_nodes(report):
        out+='<tr>'+''.join(f'<td>{esc(str(v))}</td>' for v in (row.node,row.cpu_max,row.jvm_heap_max,row.search_queue_max,row.write_queue_max,row.search_rejected_max,row.write_rejected_max))+'</tr>'
    out+='</tbody></table>'
    metric_sources=tuple(e for e in report.evidence if e.source=='node_metric' and e.provenance)
    if metric_sources:out+=f'<p class="hint">출처: {esc(metric_sources[0].provenance.table or metric_sources[0].provenance.endpoint or "node_metric")} · 관측 {esc(stamp(metric_sources[0].event_time))}</p>'
    return out+'</section>'


def _causes(narrative) -> str:
    if not narrative or not narrative.causes:
        return '<p>검증을 완료한 원인 판단 없음</p>' if not narrative else '<p>확인된 원인 없음</p>'
    return ''.join(f'<h3>{esc(cause.statement)}</h3>'+render_paragraphs(cause.mechanism) for cause in narrative.causes)


def _findings(narrative) -> str:
    # 원인이 아닌 관측은 결론에서 빼고 근거 안에서 보여 준다.
    if not narrative or not narrative.findings:
        return ''
    return '<section id="findings"><h2>주요 관측</h2>'+''.join(f'<h3>{esc(f.title)}</h3>'+render_paragraphs(f.detail) for f in narrative.findings)+'</section>'


def _actions(narrative) -> str:
    items=[str(action).strip() for action in (narrative.recommendations if narrative else ()) if str(action).strip()]
    if not items:
        return ''
    return '<h3>권장 조치</h3><ul class="actions">'+''.join(f'<li>{esc(text)}</li>' for text in items)+'</ul>'


def _citation_groups(narrative):
    """(지지, 반증) 인용. 지지와 반증에 모두 걸린 근거는 반증 쪽에만 둔다."""
    contradicting=unique_citations(*(cause.contradicting for cause in narrative.causes))
    contradicting_ids={c.evidence_id for c in contradicting}
    # 요약 문단은 보이지 않으므로 그 인용은 싣지 않는다.
    cited=tuple(c for c in unique_citations(
        *(cause.supporting for cause in narrative.causes),
        *(finding.citations for finding in narrative.findings),
        *(getattr(action,'citations',()) for action in narrative.recommendations)) if c.evidence_id not in contradicting_ids)
    return cited,contradicting


def _slowlog_marks(report, narrative) -> dict:
    """원문 섹션에 이미 나오는 slowlog 줄에 붙일 인용 표지."""
    if not narrative:
        return {}
    cited,contradicting=_citation_groups(narrative)
    shown={e.evidence_id for e in shown_slowlog_evidence(report)}
    marks={c.evidence_id:'판단 인용' for c in cited if c.evidence_id in shown}
    marks.update({c.evidence_id:'반증 인용' for c in contradicting if c.evidence_id in shown})
    return marks


def _citations(narrative, report) -> str:
    if not narrative:
        return ''
    shown={e.evidence_id for e in shown_slowlog_evidence(report)}
    cited,contradicting=_citation_groups(narrative)
    cited=tuple(c for c in cited if c.evidence_id not in shown)
    contradicting=tuple(c for c in contradicting if c.evidence_id not in shown)
    if not cited and not contradicting:
        return ''
    out='<section id="citations"><h2>판단에 인용된 원문</h2>'+quotes(cited)
    if contradicting:
        out+='<h3>반증</h3>'+''.join(f'<pre class="quote">{esc("반증 · "+text)}</pre>' for text in visible_citations(contradicting))
    return out+'</section>'


def render_layout(report: IncidentAnalysisReport, now: datetime, *, gaps: tuple[str,...], analysis_failed: bool, css: str) -> str:
    obs=report.observations
    links=build_cross_links(obs)
    rows=rank_query_requests(obs.query_requests)
    narrative=display_narrative(report,analysis_failed=analysis_failed)
    windows=obs.requested or (((report.analyzed_from,report.analyzed_to),) if report.analyzed_from and report.analyzed_to else ())
    span=' · '.join(f'{stamp(start)} ~ {stamp(end)}' for start,end in windows) or '분석 구간 미확인'
    header=f'<header><p>ClusterDoctor · {esc(report.cluster)}</p><h1>Elasticsearch 쿼리·노드 로그 분석</h1><p>{esc(span)}</p><p class="hint">생성 {esc(stamp(now))}</p>'
    if DEMO_GAP in gaps:header+='<p class="hint">합성 데이터로 실제 보고서 생성 경로를 실행한 예시입니다.</p>'
    header+='</header>'
    conclusion=render_conclusion(links,causes_html=_causes(narrative),actions_html=_actions(narrative),notices_html=_notices(obs,gaps),analysis_failed=analysis_failed)
    ranking=render_query_ranking(obs.query_requests,obs.candidates,{p.candidate_id:p.reason for p in narrative.suspect_picks} if narrative else {},query_log_note(obs))
    notes=source_log_notes(report)
    originals=''.join(f'<section id="{name}"><h2>{title}</h2>'+(f'<p class="hint">{esc(notes[name])}</p>' if name in notes else '')+''.join(f'<pre class="quote">{esc(line)}</pre>' for line in lines)+'</section>' for name,title,lines in source_log_sections(report,_slowlog_marks(report,narrative)))
    master_text=master_note(obs) if summarize_source(obs.source_statuses,'master_log').state=='failed' else ''
    evidence=('<details id="evidence"><summary>근거 보기</summary>'+_findings(narrative)+render_cross_source(links,master_note_text=master_text)+_timeline(report,narrative)
              +originals+render_ssh_logs(report.evidence,observations=obs)+_metrics(report)+_citations(narrative,report)+'</details>')
    body=(header+conclusion+render_metric_cards(rows,links)+render_query_trend(project_query_trend(obs),links)
          +ranking+evidence)
    return '<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Elasticsearch 쿼리·노드 로그 분석</title>'+PRINT_OPEN_SCRIPT+'<style>'+css+_EXTRA_CSS+'</style></head><body><main class="wrap">'+body+'</main></body></html>'
