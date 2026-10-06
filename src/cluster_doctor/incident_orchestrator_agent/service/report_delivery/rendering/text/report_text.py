"""``IncidentAnalysisReport``를 평문으로 그린다.

HTML 파일을 쓰지 못할 때 로그로 남기는 폴백이고, 길이 집계에도 쓴다.
관측 사실과 검증 게이트는 HTML과 같은 투영(``projection``)에서 가져온다.
"""

from __future__ import annotations

from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    IncidentAnalysisReport,
)


def scrub(text: str) -> str:
    """UTF-8로 인코딩할 수 없는 문자를 치환한다.

    provider 응답에 짝 없는 서로게이트가 섞여 오는 경우가 있다(JSON의
    ``\\udXXX`` 이스케이프). 그대로 두면 두 경로가 동시에 무너진다 —
    ``write_text``가 ``UnicodeEncodeError``로 실패하고, 폴백으로 전문을
    로그에 남기려 해도 파일 핸들러가 같은 이유로 실패해 리포트가 통째로
    사라진다. 글자 하나를 ``?``로 바꾸는 편이 진단을 잃는 것보다 낫다.

    치환은 입력 시점에 한 번만 한다. 렌더 결과와 원문 블록, 폴백 로그가
    모두 같은 문자열에서 나오므로 여기서 걸러야 전부 안전해진다.
    """
    return text.encode("utf-8", "replace").decode("utf-8")


def render_text(report: IncidentAnalysisReport, *, analysis_failed: bool = False) -> str:
    """Plain fallback with the same execution facts and verification gate as HTML."""
    from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import stamp, minute_stamp, report_timeline, visible_citations, evidence_text, system_maxima, timeline_sources, execution_summary, display_narrative, source_log_sections, source_log_notes
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.source_status import query_log_note, ssh_note
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.query_trend import project_query_trend
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.ssh_log_view import ssh_evidence
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import unique_citations, remaining_summary_citations
    obs=report.observations
    rows=rank_query_requests(obs.query_requests)
    out=['Elasticsearch 쿼리·노드 로그 분석', report.cluster, '핵심 요약',
        f'수집 쿼리 실행 로그 {len(rows)}건',
        f'최대 실행시간 {str(rows[0].execution_seconds)+"s" if rows and rows[0].execution_seconds is not None else "미확인"}',
        ]
    narrative=display_narrative(report,analysis_failed=analysis_failed)
    if narrative:
        out.append(narrative.headline)
    else:
        out.extend(execution_summary(obs))
        out.append('분석 해석을 확인하지 못해 관측 사실만 표시합니다.')
    out.append('쿼리 실행 추이')
    for p in project_query_trend(obs):
        out.append(f'{stamp(p.start)} ~ {stamp(p.end)} · {p.count if p.count is not None else "미확인"}건 · 최대 {str(p.maximum_seconds)+"s" if p.maximum_seconds is not None else "미확인"} · {p.status}')
    out.append('주요 타임라인')
    for card in report_timeline(report):
        out.append(f'{minute_stamp(card.start)} · {card.representative_event}')
        out.extend(item.text for item in (*card.impacts,*card.causes))
        if narrative:out.extend(item.text for item in card.interpretations)
        out.extend(visible_citations(timeline_sources(card,ssh_evidence(report.evidence))))
    out.append('느린 개별 실행 로그')
    for row in rows[:10]:
        e=row.record
        out.append(f'{stamp(e.timestamp)} · {e.cmd} · {str(row.execution_seconds)+"s" if row.execution_seconds is not None else "미확인"} · 키워드: {" · ".join(e.keyword) or "없음"} · 대상: {row.target_host or "미확인"} · 요청 호스트: {e.host}')
        out.extend(row.conditions or ('조건 미추출',))
    if not rows:out.append(query_log_note(obs))
    notes=source_log_notes(report)
    for id_,title,lines in source_log_sections(report):
        out.append(title)
        if id_ in notes:out.append(notes[id_])
        out.extend(lines)
    ssh=ssh_evidence(report.evidence)
    out.append('SSH 노드 로그')
    if ssh:
        out.extend(evidence_text(e) for e in ssh)
    else:out.append(ssh_note(obs))
    out.append('시스템 지표 · 관측 최대값')
    out.extend(system_maxima(obs.nodes))
    for n in sorted((n for n in obs.nodes if n.samples > 0),key=lambda r:(-max(r.search_queue_max,r.write_queue_max),-r.cpu_max,r.node))[:10]:
        out.append(f'{n.node} · CPU={n.cpu_max}% JVM heap={n.jvm_heap_max}% search_queue={n.search_queue_max} write_queue={n.write_queue_max} search_rejected 누적={n.search_rejected_max} write_rejected 누적={n.write_rejected_max}')
    out.append('원인 판단·조치')
    if narrative:
        for index, c in enumerate(narrative.causes):
            out.append(f'{c.statement} · 확신도 {c.confidence}')
            if c.mechanism: out.append('판단 설명: '+c.mechanism)
            out.extend('미확인 사항: '+v for v in c.uncertainties)
            actions=tuple(a for a in narrative.recommendations if getattr(a,'cause_index',None)==index)
            out.extend('확인·조치: '+str(a) for a in actions if str(a).strip())
            supporting=unique_citations(c.supporting, *(getattr(a,'citations',()) for a in actions))
            out.extend('판단 근거: '+s for s in visible_citations(supporting))
            out.extend('반증: '+s for s in visible_citations(unique_citations(c.contradicting)))
        for f in narrative.findings:
            out.extend((f.title,f.detail,*visible_citations(f.citations)))
        for a in narrative.recommendations:
            cause_index=getattr(a,'cause_index',None)
            if cause_index is not None and 0<=cause_index<len(narrative.causes):
                continue
            if str(a).strip():out.append('확인·조치: '+str(a))
            out.extend(visible_citations(unique_citations(getattr(a,'citations',()))))
        if not narrative.causes:out.append('확인된 원인 없음')
        summary_sources=remaining_summary_citations(narrative)
        if summary_sources:out.extend(('종합 요약 근거', *visible_citations(summary_sources)))
    else:out.append('검증을 완료한 원인 판단 없음')
    for s in obs.source_statuses:
        if s.status=='failed':out.append(f'{s.source} {s.host} 수집 실패 · {stamp(s.start)}')
    return scrub('\n'.join(out).rstrip()+'\n')
