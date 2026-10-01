"""Query-log ranking: the slowest request groups."""

import html

from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.query_ranking import (
    TOP_N,
    query_ranking,
    search_date,
    seconds,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import (
    scrub,
)


def esc(value: object) -> str:
    return html.escape(scrub(str(value)), quote=True)


def render_query_ranking(requests: tuple[QueryLogEntry, ...]) -> str:
    if not requests:
        return '<section id="query-ranking"><h2>검색 요청 분석</h2><p class="hint">수집된 쿼리 실행 기록 없음 · 키워드 순위를 계산할 수 없습니다.</p></section>'
    rows = []
    all_groups = query_ranking(requests)
    shown = all_groups[:TOP_N]
    for rank, group in enumerate(shown, 1):
        keyword = ", ".join(group.keywords) or "미확인"
        company, user = group.company or "미확인", group.user or "미확인"
        rows.append(
            f'<tbody class="ranking-row"><tr>'
            f'<td class="ranking-index">{rank}</td><td><b>{esc(keyword)}</b></td>'
            f"<td>{esc(company)}</td><td>{esc(user)}</td><td>{esc(group.query_type)}<br><small>cmd: {esc(group.cmd or '미확인')}</small></td>"
            f"<td>{esc(search_date(group.s_date))}<br>~ {esc(search_date(group.e_date))}<br><b>{group.date_range}일</b>"
            f"<br><small>평균 {esc(seconds(group.average))} · 최대 {esc(seconds(group.peak))}</small></td></tr>"
            f"</tbody>"
        )
    tables = sorted(
        {r.provenance.table for r in requests if r.provenance and r.provenance.table}
    )
    partial = any(r.provenance and r.provenance.excerpt for r in requests)
    notice = (
        '<p class="query-warning">부분 집계: 조회 건수 제한에 도달한 구간이 있어 전체 요청 순위와 다를 수 있습니다.</p>'
        if partial
        else ""
    )
    return (
        '<section id="query-ranking"><h2>검색 요청 분석</h2>'
        + f'<p class="hint">ClickHouse에 저장된 쿼리 실행 로그 기준 · 평균 실행 시간이 느린 순(문제 유발 후보) 상위 {len(shown)}개 조합 (전체 {len(all_groups)}개 중)</p>'
        + f'<p class="hint">수집 요청 {len(requests)}건 · 조회 테이블: {esc(", ".join(tables) or "미확인")} · 검색 시각: reg_date (KST)</p>'
        + notice
        + '<p class="hint">검색 대상 기간: s_date ~ e_date · 검색 일수: date_range · 같은 키워드라도 기간이 다르면 따로 집계합니다.</p>'
        + '<div class="query-table-scroll"><table class="query-ranking-table"><thead><tr>'
        + "".join(
            f"<th>{v}</th>"
            for v in (
                "순위",
                "키워드 조합",
                "회사",
                "사용자",
                "유형 / cmd",
                "검색 대상 기간 / 일수",
            )
        )
        + "</tr></thead>"
        + "".join(rows)
        + "</table></div></section>"
    )



QUERY_CSS = """
.query-table-scroll{overflow-x:auto;max-width:100%}.query-ranking-table{min-width:720px;width:100%}
.query-table-scroll table{border-collapse:collapse;font-size:12px}.query-table-scroll th,.query-table-scroll td{padding:12px 10px;text-align:left;border-bottom:1px solid var(--line);vertical-align:top}
.query-table-scroll th{white-space:nowrap;background:var(--surface-2)}.query-table-scroll td{overflow-wrap:anywhere}.query-table-scroll small{color:var(--ink-2)}
.query-ranking-table>tbody>tr>td:nth-child(2){min-width:220px;max-width:360px}
.query-warning{color:var(--warn);background:var(--warn-soft);padding:10px 14px;border-radius:4px}
@media print{.query-ranking-table{min-width:0}.query-table-scroll{overflow:visible}.query-table-scroll th,.query-table-scroll td{padding:5px;font-size:9px}}
"""
