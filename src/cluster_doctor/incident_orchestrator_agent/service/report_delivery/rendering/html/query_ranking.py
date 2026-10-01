"""Query-log ranking: the slowest request groups."""

from decimal import Decimal

from cluster_doctor.incident_analysis_agent.model.log_entries import QueryLogEntry
from cluster_doctor.incident_analysis_agent.model.observations import SlowCandidate
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.query_ranking import (
    QueryRanking,
    query_ranking,
    search_date,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.evidence_link import (
    esc,
)

TOP_N = 5
COLUMNS = ("ID", "Query", "Cmd", "Range", "Avg", "Max")


def seconds_or_dash(value: Decimal | None) -> str:
    return f"{value:.3f}초" if value is not None else "—"


def candidate_ids(
    groups: tuple[QueryRanking, ...], candidates: tuple[SlowCandidate, ...]
) -> dict[int, list[str]]:
    """Map group index to candidate ids.

    A candidate carries no group key, so it is tied to a group only when one of
    the group's requests equals it on every field both share (reg_date, host,
    run_time, cmd, company, user) and no other group does. Otherwise no id.
    """
    found: dict[int, list[str]] = {}
    for c in candidates:
        if c.source != QueryLogEntry.source or not c.candidate_id:
            continue
        matches = [
            i
            for i, g in enumerate(groups)
            if any(
                r.timestamp == c.timestamp
                and r.host == c.node
                and r.run_time == c.run_time
                and r.cmd == c.cmd
                and (r.company or "") == c.company
                and (r.user or "") == c.user
                for r in g.requests
            )
        ]
        if len(matches) == 1:
            found.setdefault(matches[0], []).append(c.candidate_id)
    return found


def render_query_ranking(
    requests: tuple[QueryLogEntry, ...],
    candidates: tuple[SlowCandidate, ...] = (),
    picks: dict[str, str] | None = None,
) -> str:
    if not requests:
        return '<section id="query-ranking"><h2>의심 요청</h2><p class="hint">수집된 쿼리 실행 기록 없음 · 키워드 순위를 계산할 수 없습니다.</p></section>'
    picks = picks or {}
    all_groups = query_ranking(requests)
    shown = all_groups[:TOP_N]
    # Matched against every group so a twin outside the top N still blocks attribution.
    ids = candidate_ids(all_groups, candidates)
    bodies = []
    for i, group in enumerate(shown):
        row_ids = ids.get(i, [])
        reasons = [(cid, picks[cid]) for cid in row_ids if cid in picks]
        owner = " · ".join(filter(None, (group.company, group.user)))
        picked = ' class="picked"' if reasons else ""
        period = f"{search_date(group.s_date)} ~ {search_date(group.e_date)}"
        bodies.append(
            f"<tbody{picked}><tr>"
            f'<td class="mono">{esc(", ".join(row_ids) or "—")}</td>'
            f'<td><b class="mono">{esc(", ".join(group.keywords) or "—")}</b>'
            + (f'<br><span class="hint">{esc(owner)}</span>' if owner else "")
            + "</td>"
            f"<td>{esc(group.cmd or '—')}</td>"
            f'<td class="mono"><span title="{esc(period)}">{group.date_range}d</span></td>'
            f'<td class="mono">{esc(seconds_or_dash(group.average))}</td>'
            f'<td class="mono">{esc(seconds_or_dash(group.peak))}</td></tr>'
            + "".join(
                f'<tr class="pick-reason"><td></td><td colspan="5">'
                f'<span class="model-tag">판단</span> {esc(reason)}</td></tr>'
                for _, reason in reasons
                if reason
            )
            + "</tbody>"
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
        '<section id="query-ranking"><h2>의심 요청</h2>'
        + f'<p class="hint">ClickHouse에 저장된 쿼리 실행 로그 기준 · 평균 실행 시간이 느린 순(문제 유발 후보) 상위 {len(shown)}개 조합 (전체 {len(all_groups)}개 중)</p>'
        + f'<p class="hint">수집 요청 {len(requests)}건 · 조회 테이블: {esc(", ".join(tables) or "미확인")} · 검색 시각: reg_date (KST)</p>'
        + notice
        + '<p class="hint">Range: 검색 대상 기간(date_range) · 같은 키워드라도 기간이 다르면 따로 집계합니다.</p>'
        + '<div class="query-table-scroll"><table class="query-ranking-table"><thead><tr>'
        + "".join(f"<th>{v}</th>" for v in COLUMNS)
        + "</tr></thead>"
        + "".join(bodies)
        + "</table></div></section>"
    )
