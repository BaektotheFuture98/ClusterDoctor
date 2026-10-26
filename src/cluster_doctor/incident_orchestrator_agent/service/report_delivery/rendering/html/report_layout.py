"""코드가 만든 사실과 검증된 해석으로 구성한 독립 실행형 리포트.

본문은 결론(판정·원인·조치)·지표·추이·느린 요청만 둔다. 소스 간 연결, 원문 로그,
인용 근거는 "근거 보기" 안에 접는다. 결론을 의심할 때 펼쳐 보는 자료다.
"""
from datetime import datetime
from cluster_doctor.incident_analysis_agent.service.observation.correlation import build_cross_links
from cluster_doctor.incident_orchestrator_agent.model.incident_report import IncidentAnalysisReport
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import stamp, minute_stamp, report_timeline, visible_citations, system_maxima, timeline_sources, display_narrative, withheld_reason, partial_verification_notice, analysis_span, source_log_sections, source_log_notes, shown_slowlog_evidence, metric_nodes, unique_citations
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.query_trend import project_query_trend
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.cross_links_text import cluster_event_line, node_line, verdict_line
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.source_status import query_log_note
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
#overview table{width:auto;margin-top:12px}
#overview th{background:none;font-weight:600;padding:6px 16px 6px 0;border:none;width:160px}
#overview td{border:none;padding:6px 0}
.wiki-badge{font-size:11px;background:#e8f0fe;color:#1a56a5;border-radius:4px;padding:2px 6px;margin-left:6px;white-space:nowrap}
section p{max-width:none}
#incident-analysis h3{margin:16px 0 8px;font-size:16px;color:#52657b;border-bottom:1px solid #dde4ef;padding-bottom:4px}
#incident-analysis h4{margin:10px 0 4px;font-size:14px}
@media(max-width:700px){.wrap{padding:12px}section{padding:14px}table{font-size:11px}}
@media print{body{background:white}.wrap{max-width:none;padding:0}section{border-radius:0;box-shadow:none}svg{display:block!important}thead{display:table-header-group}tr,.quote{break-inside:avoid}pre{overflow:visible}a{text-decoration:none}details#evidence>*{display:block}details::details-content{content-visibility:visible;display:block}}
'''


def _overview(report: IncidentAnalysisReport, *, now: datetime | None = None) -> str:
    title = report.title or "Elasticsearch 클러스터 장애 분석"
    span_from, span_to = analysis_span(report)
    start = stamp(span_from) if span_from else "미확인"
    end = stamp(span_to) if span_to else "미확인"
    duration = ""
    if span_from and span_to:
        delta = span_to - span_from
        total_minutes = int(delta.total_seconds() // 60)
        duration = f"{total_minutes}분" if total_minutes >= 1 else f"{int(delta.total_seconds())}초"
    fallback_hint = (
        '<p class="hint">AI 분석이 없어 수집 요청 구간 기준으로 표시했습니다.</p>'
        if span_from and not report.analyzed_from else ''
    )
    generated =f'<tr><th>보고서 생성</th><td class="hint">{esc(stamp(now))}</td></tr>' if now else ''
    return (
        f'<section id="overview">'
        f'<h1>{esc(title)}</h1>'
        f'<table><tbody>'
        f'<tr><th>발생 및 인지 일시</th><td>{esc(start)}</td></tr>'
        f'<tr><th>복구 완료 일시</th><td>{esc(end)}</td></tr>'
        f'<tr><th>총 장애 시간</th><td>{esc(duration or "미확인")}</td></tr>'
        + generated +
        f'</tbody></table>'
        + fallback_hint +
        f'</section>'
    )


def _verdict_block(links) -> str:
    """코드가 판정한 지연 범위·노드 응답 지연·클러스터 이벤트. AI 해석과 무관하게 항상 그린다."""
    v = verdict_line(links)
    out = f'<p class="verdict">{esc(v.headline)}</p>'
    if v.detail:
        out += f'<p class="hint">{esc(v.detail)}</p>'
    if line := node_line(links):
        out += f'<p class="verdict">{esc(line)}</p>'
    if line := cluster_event_line(links):
        out += f'<p class="hint">{esc(line)}</p>'
    return out


def _incident_analysis(report: IncidentAnalysisReport, narrative, links, withheld: str = '', partial: str = '') -> str:
    out = '<section id="incident-analysis"><h2>현상 및 원인</h2>' + _verdict_block(links)
    if withheld:
        out += f'<p class="hint">{esc(withheld)}</p>'
    else:
        if partial:
            out += f'<p class="hint">{esc(partial)}</p>'
        if report.summary:
            out += render_paragraphs(report.summary)
    if not narrative.causes:
        return out + '<p>확인된 원인 없음</p></section>'
    direct = [c for c in narrative.causes if c.cause_type == "direct"]
    root = [c for c in narrative.causes if c.cause_type != "direct"]
    if direct:
        out += '<h3>직접적 원인</h3>'
        for c in direct:
            out += f'<h4>{esc(c.statement)}</h4>' + render_paragraphs(c.mechanism)
    if root:
        out += '<h3>근본적 원인</h3>'
        for c in root:
            out += f'<h4>{esc(c.statement)}</h4>' + render_paragraphs(c.mechanism)
    return out + '</section>'


def _recommendations_section(narrative) -> str:
    items = [
        (str(a).strip(), getattr(a, 'wiki_ref', ''))
        for a in (narrative.recommendations if narrative else ())
    ]
    items = [(text, ref) for text, ref in items if text]
    if not items:
        return ''
    out = '<section id="recommendations"><h2>권장 조치</h2><ul class="actions">'
    for text, wiki_ref in items:
        badge = (
            f' <span class="wiki-badge">위키: {esc(wiki_ref)}</span>'
            if wiki_ref else ''
        )
        out += f'<li>{esc(text)}{badge}</li>'
    return out + '</ul></section>'


def _future_improvements(report: IncidentAnalysisReport) -> str:
    items = [s.strip() for s in report.future_improvements if s.strip()]
    if not items:
        return ''
    return (
        '<section id="future-improvements"><h2>향후 개선 방안</h2><ul>'
        + ''.join(f'<li>{esc(item)}</li>' for item in items)
        + '</ul></section>'
    )


def quotes(items):
    return ''.join(f'<pre class="quote">{esc(text)}</pre>' for text in visible_citations(items))


def render_paragraphs(text: str, *, css_class: str = '') -> str:
    """작성된 문단을 주장이나 마크업으로 해석하지 않고 그대로 표시한다."""
    attribute = f' class="{esc(css_class)}"' if css_class else ''
    return ''.join(f'<p{attribute}>{esc(paragraph.strip())}</p>'
                   for paragraph in text.replace('\r\n', '\n').split('\n\n') if paragraph.strip())


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


def _citation_groups(narrative):
    """(지지, 반증) 인용. 지지와 반증에 모두 걸린 근거는 반증 쪽에만 둔다."""
    contradicting=unique_citations(*(cause.contradicting for cause in narrative.causes))
    contradicting_ids={c.evidence_id for c in contradicting}
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


def render_layout(report: IncidentAnalysisReport, now: datetime, *, gaps: tuple[str,...], analysis_failed: bool, css: str) -> str:
    obs=report.observations
    links=build_cross_links(obs)
    narrative=display_narrative(report,analysis_failed=analysis_failed)
    withheld=withheld_reason(report,analysis_failed=analysis_failed)
    partial='' if withheld else partial_verification_notice(report)
    ranking=render_query_ranking(obs.query_requests,obs.candidates,{p.candidate_id:p.reason for p in narrative.suspect_picks} if narrative else {},query_log_note(obs))
    notes=source_log_notes(report)
    originals=''.join(f'<section id="{name}"><h2>{title}</h2>'+(f'<p class="hint">{esc(notes[name])}</p>' if name in notes else '')+''.join(f'<pre class="quote">{esc(line)}</pre>' for line in lines)+'</section>' for name,title,lines in source_log_sections(report,_slowlog_marks(report,narrative)))
    evidence=('<details id="evidence"><summary>수집 데이터 보기</summary>'
              +ranking+originals+render_ssh_logs(report.evidence,observations=obs)+_metrics(report)+'</details>')
    body=(_overview(report, now=now)
          +render_query_trend(project_query_trend(obs),links)
          +_timeline(report,narrative)
          +_incident_analysis(report, narrative, links, withheld, partial)
          +_recommendations_section(narrative)
          +('' if withheld else _future_improvements(report))
          +evidence)
    return '<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Elasticsearch 쿼리·노드 로그 분석</title>'+PRINT_OPEN_SCRIPT+'<style>'+css+_EXTRA_CSS+'</style></head><body><main class="wrap">'+body+'</main></body></html>'
