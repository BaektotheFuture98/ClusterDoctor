"""Inspectable query-log rankings with local filtering and sorting."""

import html

from cluster_doctor.incident_analysis_agent.model.log_entries import (
    QueryLogEntry,
    record_json,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.evidence_citation import (
    kst_stamp,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.query_ranking import (
    duration,
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
    for rank, group in enumerate(query_ranking(requests), 1):
        keyword = ", ".join(group.keywords) or "미확인"
        company, user = group.company or "미확인", group.user or "미확인"
        ordered = sorted(
            group.requests,
            key=lambda r: (duration(r.run_time) or 0, r.timestamp),
            reverse=True,
        )
        # Statistics use every fetched row. Detail previews are explicitly bounded.
        details = []
        for r in ordered[:20]:
            p = r.provenance
            source = p.table if p and p.table else "수집 위치 미확인"
            details.append(
                "<tr>"
                + "".join(
                    f"<td>{esc(v)}</td>"
                    for v in (
                        kst_stamp(r.timestamp),
                        seconds(duration(r.run_time)),
                        r.cmd or "미확인",
                        {"Y": "성공 (Y)", "N": "실패 (N)"}.get(
                            r.success, f"미확인 ({r.success})"
                        ),
                        r.host or "미확인",
                        r.service or "미확인",
                        r.project or "미확인",
                        r.env or "미확인",
                        r.cluster or "미확인",
                    )
                )
                + f'</tr><tr><td colspan="9"><p class="request-context">검색 대상: {esc(search_date(r.s_date))} ~ {esc(search_date(r.e_date))} · date_range: {r.date_range}일 · keyword_count: {r.keyword_count} · search_count: {r.search_count} · URL: {esc(r.url)}</p><details class="request-record"><summary>ClickHouse 수집 레코드 · '
                + esc(source)
                + '</summary><pre class="raw">'
                + esc(record_json(r))
                + "</pre></details></td></tr>"
            )
        detail_html = (
            '<details class="request-list"><summary>개별 요청 · 실행 시간순 상위 '
            + str(min(20, group.count))
            + "/"
            + str(group.count)
            + "건</summary>"
            + '<p class="hint">검색 시각은 reg_date, 검색 대상 기간은 s_date/e_date, 검색 일수는 date_range 원본 값을 표시합니다.</p>'
            + '<div class="query-table-scroll"><table><thead><tr>'
            + "".join(
                f"<th>{v}</th>"
                for v in (
                    "검색 시각(reg_date, KST)",
                    "실행 시간",
                    "cmd",
                    "결과",
                    "호스트",
                    "서비스",
                    "프로젝트",
                    "환경",
                    "클러스터",
                )
            )
            + "</tr></thead>"
            + "".join(details)
            + "</tbody></table></div></details>"
        )
        first, last = kst_stamp(group.first), kst_stamp(group.last)
        avg = str(group.average) if group.average is not None else ""
        peak = str(group.peak) if group.peak is not None else ""
        total = str(group.total) if group.total is not None else ""
        rows.append(
            f'<tbody class="ranking-row" data-kind="{esc(group.query_type)}" data-count="{group.count}" '
            f'data-average="{esc(avg)}" data-peak="{esc(peak)}" data-total="{esc(total)}" '
            f'data-first="{group.first.timestamp()}" data-last="{group.last.timestamp()}"><tr>'
            f'<td class="ranking-index">{rank}</td><td><b>{esc(keyword)}</b></td>'
            f"<td>{esc(company)}</td><td>{esc(user)}</td><td>{esc(group.query_type)}<br><small>cmd: {esc(group.cmd or '미확인')}</small></td>"
            f"<td>{esc(search_date(group.s_date))}<br>~ {esc(search_date(group.e_date))}<br><b>{group.date_range}일</b></td>"
            f"<td>{group.count}<br><small>실패 {sum(r.is_success is False for r in group.requests)}건</small></td>"
            f"<td>{esc(seconds(group.average))}<br><small>유효 {group.valid_count}건</small></td>"
            f"<td>{esc(seconds(group.peak))}</td><td>{esc(seconds(group.total))}</td>"
            f'<td>{esc(first)}<br>~ {esc(last)}</td></tr><tr><td colspan="11">{detail_html}</td></tr></tbody>'
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
        '<p class="hint">ClickHouse에 저장된 쿼리 실행 로그 기준 · 키워드 조합/회사/사용자/cmd별 집계 · 평균 실행 시간 내림차순</p>'
        + f'<p class="hint">수집 요청 {len(requests)}건 · 조회 테이블: {esc(", ".join(tables) or "미확인")} · 검색 시각: reg_date (KST)</p>'
        + notice
        + '<p class="hint">검색 대상 기간: s_date ~ e_date · 검색 일수: date_range · 같은 키워드라도 기간이 다르면 따로 집계합니다.</p>'
        + '<details class="source-details"><summary>집계 기준</summary><p class="hint">여러 키워드는 하나의 조합으로 집계하며 실행 시간을 개별 키워드에 중복 배분하지 않습니다. 공통 요청 ID가 없어 같은 전체 기록은 중복 조회 시 관측된 최대 건수를 유지합니다. 평균·합계는 유효한 실행 시간 기록을 기준으로 계산합니다.</p></details>'
        + '<div class="query-controls"><label>키워드·회사·사용자 검색 <input id="query-filter" type="search" placeholder="검색어 입력"></label>'
        '<label>쿼리 유형 <select id="query-kind"><option value="">전체</option><option>search</option><option>agg</option><option>기타</option></select></label>'
        '<label>정렬 <select id="query-sort"><option value="average">평균 실행 시간 ↓</option><option value="peak">최대 실행 시간 ↓</option><option value="total">총 실행 시간 ↓</option><option value="count">요청 수 ↓</option><option value="first">첫 검색 시각 ↑</option><option value="last">마지막 검색 시각 ↓</option></select></label></div>'
        '<p id="query-result-count" class="hint" aria-live="polite"></p><div class="query-table-scroll"><table class="query-ranking-table"><thead><tr>'
        + "".join(
            f"<th>{v}</th>"
            for v in (
                "순위",
                "키워드 조합",
                "회사",
                "사용자",
                "유형 / cmd",
                "검색 대상 기간 / 일수",
                "요청 수",
                "평균",
                "최대",
                "합계",
                "첫·마지막 검색 시각(KST)",
            )
        )
        + "</tr></thead>"
        + "".join(rows)
        + "</table></div></section>"
        + QUERY_SCRIPT
    )


QUERY_CSS = """
.query-controls{display:flex;flex-wrap:wrap;gap:12px;margin:20px 0}.query-controls label{display:flex;flex-direction:column;gap:5px;font-size:12px;color:var(--ink-2)}
.query-controls input,.query-controls select{font:inherit;font-size:14px;padding:8px;border:1px solid var(--line);border-radius:4px;color:var(--ink);background:var(--surface)}
.query-table-scroll{overflow-x:auto;max-width:100%}.query-ranking-table{min-width:1250px;width:100%}
.query-table-scroll table{border-collapse:collapse;font-size:12px}.query-table-scroll th,.query-table-scroll td{padding:12px 10px;text-align:left;border-bottom:1px solid var(--line);vertical-align:top}
.query-table-scroll th{white-space:nowrap;background:var(--surface-2)}.query-table-scroll td{overflow-wrap:anywhere}.query-table-scroll small{color:var(--ink-2)}
.request-list>summary,.request-record>summary{cursor:pointer;font-size:12px;color:var(--accent-ink);margin-top:8px}.query-ranking-table td:nth-child(2){min-width:220px;max-width:360px}
.request-context{font-size:12px;color:var(--ink-2);padding:8px 0;overflow-wrap:anywhere}.request-list[open]{width:100%}.request-list table{min-width:850px;width:100%}.ranking-row[hidden]{display:none}.query-warning{color:var(--warn);background:var(--warn-soft);padding:10px 14px;border-radius:4px}
@media print{.query-controls{display:none}.query-ranking-table{min-width:0}.query-table-scroll{overflow:visible}.query-table-scroll th,.query-table-scroll td{padding:5px;font-size:9px}}
"""

QUERY_SCRIPT = """<script>
(() => {
 const section = document.getElementById('query-ranking');
 const body = section.querySelector('.query-ranking-table');
 const rows = Array.from(body.tBodies);
 const filter = section.querySelector('#query-filter'), kind = section.querySelector('#query-kind'), sort = section.querySelector('#query-sort');
 const searchable = new Map(rows.map(row => [row, Array.from(row.rows[0].children).slice(1,5).map(cell => cell.firstChild.textContent).join(' ').toLocaleLowerCase()]));
 function update() {
  const key = sort.value, q = filter.value.toLocaleLowerCase();
  rows.sort((a,b) => {
   const x = a.dataset[key] === '' ? null : Number(a.dataset[key]);
   const y = b.dataset[key] === '' ? null : Number(b.dataset[key]);
   if (x === null) return y === null ? 0 : 1;
   if (y === null) return -1;
   return key === 'first' ? x-y : y-x;
  });
  let count = 0;
  rows.forEach(row => {
   row.hidden = !searchable.get(row).includes(q) || (kind.value !== '' && row.dataset.kind !== kind.value);
   if (!row.hidden) row.querySelector('.ranking-index').textContent = String(++count);
   body.appendChild(row);
  });
  section.querySelector('#query-result-count').textContent = `표시 ${count} / ${rows.length}개 조합`;
 }
 filter.addEventListener('input',update);kind.addEventListener('change',update);sort.addEventListener('change',update);update();
})();
</script>"""
