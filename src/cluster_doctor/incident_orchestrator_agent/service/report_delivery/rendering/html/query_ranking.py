"""Rank individual execution logs, never partial-keyword groups."""
from cluster_doctor.incident_analysis_agent.service.observation.query_requests import rank_query_requests, matching_candidate
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import stamp
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.source_status import QUERY_LOG_MISSING
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.evidence_link import esc

TOP_N=10

def render_query_ranking(requests, candidates=(), picks=None, empty_note=QUERY_LOG_MISSING):
    rows=[]
    for row in rank_query_requests(tuple(requests))[:TOP_N]:
        entry=row.record
        keyword=' · '.join(entry.keyword) or '저장된 키워드 없음'
        if entry.keyword_omitted:keyword+=f' (+{entry.keyword_omitted}개 생략)'
        condition='\n'.join(row.conditions) or '조건 미추출'
        condition+=f'\ns_date={entry.s_date} e_date={entry.e_date} date_range={entry.date_range}\ncompany={entry.company} user={entry.user}'
        if entry.provenance:
            condition+=f'\n출처: {entry.provenance.table or entry.provenance.endpoint or entry.provenance.method}'
            if entry.provenance.excerpt:condition+=' · 부분 수집'
        candidate=matching_candidate(entry,tuple(requests),tuple(candidates))
        if candidate and picks and candidate.candidate_id in picks:
            condition+='\n선정 이유: '+picks[candidate.candidate_id]
        target=f'{row.target_host or "대상 미확인"} / {row.index_name or "인덱스 미확인"}\n요청 호스트: {entry.host or "미확인"}'
        values=(stamp(entry.timestamp),entry.cmd,keyword,target,condition,f'{row.execution_seconds}s' if row.execution_seconds is not None else '미확인')
        rows.append('<tr class="execution-row">'+''.join(f'<td>{esc(v)}</td>' for v in values)+'</tr>')
    return '<section id="query-ranking"><h2>느린 개별 실행 로그</h2>'+('<table><thead><tr><th>로그 시각</th><th>명령</th><th>저장된 키워드</th><th>대상</th><th>조건</th><th>실행시간</th></tr></thead><tbody>'+''.join(rows)+'</tbody></table>' if rows else f'<p class="hint">{esc(empty_note)}</p>')+'</section>'
