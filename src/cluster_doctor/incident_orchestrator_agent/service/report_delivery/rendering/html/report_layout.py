"""Actual standalone report using code facts and verified interpretations."""
from datetime import datetime
from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests
from cluster_doctor.incident_orchestrator_agent.model.incident_report import IncidentAnalysisReport
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import stamp, minute_stamp, report_timeline, visible_citations, evidence_text, system_maxima, timeline_sources, execution_summary, display_narrative, source_log_sections, source_log_notes, is_validation_diagnostic
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.query_trend import project_query_trend
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.evidence_link import esc
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.source_status import query_log_note
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.query_ranking import render_query_ranking
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.query_trend import render_query_trend
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.ssh_log_view import render_ssh_logs, ssh_evidence

DEMO_GAP = "디자인 미리보기용 가상 데이터입니다. 실제 장애 분석 결과가 아닙니다."

_EXTRA_CSS='''
body{background:#f2f5fa;color:#192b43;font-family:system-ui,sans-serif}.wrap{max-width:1180px;margin:auto;padding:28px}
section{background:white;border:1px solid #dde4ef;border-radius:12px;padding:24px;margin:20px 0}
h1{font-size:28px}h2{font-size:21px}.metrics{display:flex;gap:32px;flex-wrap:wrap}.metric strong{display:block;font-size:28px;color:#1555aa}
table{width:100%;border-collapse:collapse;table-layout:fixed;font-size:13px}th,td{padding:10px;border-bottom:1px solid #dde4ef;text-align:left;vertical-align:top;overflow-wrap:anywhere;white-space:pre-wrap}th{background:#eef3fb}
pre{white-space:pre-wrap;overflow-wrap:anywhere;font:12px/1.65 ui-monospace,monospace;margin:0}svg{width:100%;height:auto}svg text{font-size:12px;fill:#35506b}.hint{color:#52657b}
.timeline-event h3{margin:0 0 8px}.quote{background:#f5f7fb;padding:12px;border-left:3px solid #adc8ee;margin:10px 0}
@media(max-width:700px){.wrap{padding:12px}section{padding:14px}table{font-size:11px}}
@media print{body{background:white}.wrap{max-width:none;padding:0}section{border-radius:0;box-shadow:none}svg{display:block!important}thead{display:table-header-group}tr,.quote{break-inside:avoid}pre{overflow:visible}a{text-decoration:none}}
'''


def quotes(items):
    return ''.join(f'<pre class="quote">{esc(text)}</pre>' for text in visible_citations(items))


def render_layout(report: IncidentAnalysisReport, now: datetime, *, gaps: tuple[str,...], analysis_failed: bool, css: str) -> str:
    obs=report.observations
    rows=rank_query_requests(obs.query_requests)
    maximum=f'{rows[0].execution_seconds}s' if rows and rows[0].execution_seconds is not None else '미확인'
    narrative=display_narrative(report,analysis_failed=analysis_failed)
    windows=obs.requested or (((report.analyzed_from,report.analyzed_to),) if report.analyzed_from and report.analyzed_to else ())
    span=' · '.join(f'{stamp(start)} ~ {stamp(end)}' for start,end in windows) or '분석 구간 미확인'
    header=f'<header><p>ClusterDoctor · {esc(report.cluster)}</p><h1>Elasticsearch 쿼리·노드 로그 분석</h1><p>{esc(span)}</p><p class="hint">생성 {esc(stamp(now))}</p>'
    if DEMO_GAP in gaps:header+='<p class="hint">합성 데이터로 실제 보고서 생성 경로를 실행한 예시입니다.</p>'
    header+='</header>'
    summary='<section id="summary"><h2>핵심 요약</h2><div class="metrics">'+f'<div class="metric">수집 쿼리 실행 로그<strong>{len(rows)}건</strong></div><div class="metric">최대 실행시간<strong>{esc(maximum)}</strong></div></div>'
    if rows and rows[0].execution_seconds is not None:summary+=f'<p>가장 느린 실행: {esc(stamp(rows[0].record.timestamp))} · {esc(rows[0].record.cmd)}</p>'
    if narrative and narrative.headline:summary+=f'<p class="report-headline">{esc(narrative.headline)}</p>'+quotes(narrative.headline_citations)
    if analysis_failed:summary+='<p class="hint">분석 실패</p>'
    failures=[s for s in obs.source_statuses if s.status=='failed']
    for status in failures:summary+=f'<p class="hint">{esc(status.source)} {esc(status.host)} 수집 실패 · {esc(stamp(status.start))}</p>'
    # Existing exception-only callers still retain their collection failure notice.
    for gap in dict.fromkeys(gaps):
        if gap != DEMO_GAP and not is_validation_diagnostic(gap):summary+=f'<p class="hint">{esc(gap)}</p>'
    if any(row.failed for row in obs.timeline):summary+='<p class="hint">분석하지 못한 구간이 있습니다.</p>'
    summary+='</section>'
    timeline='<section id="timeline"><h2>주요 타임라인</h2><div class="incident-timeline">'
    for card in report_timeline(report):
        severity_class={'Critical':' timeline-event-critical','Warning':' timeline-event-warning'}.get(card.severity,'')
        timeline+=f'<article class="timeline-event{severity_class}"><div class="event-time"><time datetime="{esc(card.start.isoformat())}">{esc(minute_stamp(card.start))}</time>'+(f'<time datetime="{esc(card.end.isoformat())}">마지막 관측 {esc(minute_stamp(card.end))}</time>' if card.end!=card.start else '')+'</div><div>'+f'<h3>{esc(card.representative_event)}</h3>'
        for item in (*card.impacts,*card.causes):timeline+=f'<p>{esc(item.text)}</p>'
        if narrative:
            for item in card.interpretations:timeline+=f'<p class="analysis-note">{esc(item.text)}</p>'
        timeline+=quotes(timeline_sources(card,ssh_evidence(report.evidence)))
        timeline+='</div></article>'
    timeline+='</div></section>'
    metrics='<section id="system-metrics"><h2>시스템 지표 · 관측 최대값</h2>'
    nodes=tuple(n for n in obs.nodes if n.samples > 0)
    if nodes:
        metrics+=''.join(f'<p>{esc(value)}</p>' for value in system_maxima(nodes))
        metrics+='<table><thead><tr><th>ES 노드</th><th>CPU(%)</th><th>JVM heap(%)</th><th>Search 큐</th><th>Write 큐</th><th>Search rejected 누적</th><th>Write rejected 누적</th></tr></thead><tbody>'
        for row in sorted(nodes,key=lambda r:(-max(r.search_queue_max,r.write_queue_max),-r.cpu_max,r.node))[:10]:
            metrics+='<tr>'+''.join(f'<td>{esc(str(v))}</td>' for v in (row.node,row.cpu_max,row.jvm_heap_max,row.search_queue_max,row.write_queue_max,row.search_rejected_max,row.write_rejected_max))+'</tr>'
        metrics+='</tbody></table>'
        metric_sources=tuple(e for e in report.evidence if e.source=='node_metric' and e.provenance)
        if metric_sources:metrics+=f'<p class="hint">출처: {esc(metric_sources[0].provenance.table or metric_sources[0].provenance.endpoint or "node_metric")} · 관측 {esc(stamp(metric_sources[0].event_time))}</p>'
    else:metrics+='<p>수집된 시스템 지표 없음</p>'
    metrics+='</section>'
    causes='<section id="causes"><h2>원인 판단·조치</h2>'
    if narrative:
        for cause in narrative.causes:
            causes+=f'<h3>{esc(cause.statement)}</h3><p>확신도: {esc(cause.confidence or "미확인")}</p><p>판단 근거</p>'+quotes(cause.supporting)
            if cause.contradicting:causes+='<p>반증</p>'+quotes(cause.contradicting)
        for finding in narrative.findings:causes+=f'<h3>{esc(finding.title)}</h3><p>{esc(finding.detail)}</p>'+quotes(finding.citations)
        for action in narrative.recommendations:causes+=f'<p>확인·조치: {esc(str(action))}</p>'+quotes(getattr(action,'citations',()))
        if not narrative.causes:causes+='<p>확인된 원인 없음</p>'
    else:causes+='<p>검증을 완료한 원인 판단 없음</p>'
    causes+='</section>'
    notes=source_log_notes(report)
    originals=''.join(f'<section id="{name}"><h2>{title}</h2>'+(f'<p class="hint">{esc(notes[name])}</p>' if name in notes else '')+''.join(f'<pre class="quote">{esc(line)}</pre>' for line in lines)+'</section>' for name,title,lines in source_log_sections(report))
    footer=''
    return '<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Elasticsearch 쿼리·노드 로그 분석</title><style>'+css+_EXTRA_CSS+'</style></head><body><main class="wrap">'+header+summary+render_query_trend(project_query_trend(obs))+timeline+render_query_ranking(obs.query_requests,obs.candidates,{p.candidate_id:p.reason for p in narrative.suspect_picks} if narrative else {},query_log_note(obs))+originals+render_ssh_logs(report.evidence,observations=obs)+metrics+causes+footer+'</main></body></html>'
