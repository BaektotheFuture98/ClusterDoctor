"""Operator-first layout: conclusion, chronological events, and original evidence."""

import hashlib
import html
from datetime import datetime

from cluster_doctor.incident_analysis_agent.model.observations import observed_severity
from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    IncidentAnalysisReport,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.evidence_citation import (
    EvidenceCitation,
    citation_lines,
    citations,
    kst_stamp,
    raw_label,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.query_ranking import (
    QUERY_CSS,
    render_query_ranking,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import (
    candidate_details,
    candidate_line,
    health_lines,
    master_log_lines,
    node_lines,
    offender_lines,
    overview_lines,
    projected_timeline,
    render_text,
    scrub,
    timeline_line,
)


def esc(value: str) -> str:
    return html.escape(scrub(value), quote=True)


def anchor(ref: str) -> str:
    return (
        "evidence-"
        + hashlib.sha256(ref.encode("utf-8", errors="surrogatepass")).hexdigest()
    )


def ref_links(refs: tuple[str, ...]) -> str:
    return " ".join(
        f'<a href="#{anchor(ref)}">{esc(ref)}</a>' for ref in dict.fromkeys(refs)
    )


def pre(text: str) -> str:
    return f'<pre class="raw"><code>{esc(text)}</code></pre>'


class CitationRenderer:
    def __init__(self):
        self.seen: set[str] = set()

    def render_unseen(self, items: tuple[EvidenceCitation, ...]) -> str:
        return self.render(
            tuple(item for item in items if item.evidence_id not in self.seen)
        )

    def render(
        self,
        items: tuple[EvidenceCitation, ...],
        *,
        compact: bool = False,
        event_start: datetime | None = None,
    ) -> str:
        out = []
        for c in items:
            identifier = ""
            if c.evidence_id not in self.seen:
                identifier = f' id="{anchor(c.evidence_id)}"'
                self.seen.add(c.evidence_id)
            metadata = citation_lines(c)
            e = c.evidence
            raw = (e.raw or e.message) if e else ""
            lines = raw.splitlines(keepends=True)
            preview_lines = 3 if compact else 12
            content = pre("".join(lines[:preview_lines])) if raw else ""
            if compact:
                source = str(e.source) if e else "출처 미확인"
                if e and e.provenance:
                    p = e.provenance
                    source = " · ".join(
                        filter(
                            None,
                            (
                                p.method.upper() if p.method == "ssh" else p.method,
                                p.host,
                                p.table,
                                p.file_path,
                                p.endpoint,
                            ),
                        )
                    )
                stamp = kst_stamp(e.event_time) if e else "시각 미확인"
                if e and event_start and e.event_time < event_start:
                    stamp = "선행 비교 근거 · " + stamp
                node = (
                    (e.node_name or e.node_id or "대상 미확인") if e else "대상 미확인"
                )
                attention = [
                    line
                    for line in metadata
                    if line.startswith(("시각 주의", "일부 발췌", "수집 범위"))
                ]
                out.append(
                    f'<div class="evidence-block compact-evidence"{identifier}>'
                    f'<p class="evidence-title">{esc(stamp)} · {esc(node)} · [{esc(c.evidence_id)}]</p>'
                    f'<p class="source-location">{esc(source)}</p>'
                    + "".join(f'<p class="hint">{esc(line)}</p>' for line in attention)
                    + f'<p class="raw-label">{esc(raw_label(c))}</p>{content}'
                    + '<details class="source-details"><summary>전체 원문 · 수집 정보</summary>'
                    + '<div class="evidence-meta">'
                    + "".join(f"<div>{esc(line)}</div>" for line in metadata[1:])
                    + '</div><details class="log-remainder" open><summary>원문</summary>'
                    + pre(raw)
                    + "</details></details></div>"
                )
                continue
            if len(lines) > 12:
                content += (
                    '<details class="log-remainder"><summary>전체 보기 · 나머지 원문</summary>'
                    + pre("".join(lines[12:]))
                    + "</details>"
                )
            out.append(
                f'<div class="evidence-block"{identifier}>'
                f'<p class="evidence-title">{esc(metadata[0])}</p>'
                '<div class="evidence-meta">'
                + "".join(f"<div>{esc(line)}</div>" for line in metadata[1:])
                + "</div>"
                f'<p class="raw-label">{esc(raw_label(c))}</p>{content}</div>'
            )
        return "".join(out)


def bullet_list(items: tuple[str, ...] | list[str]) -> str:
    return "<ul>" + "".join(f"<li>{esc(item)}</li>" for item in items) + "</ul>"


def confidence(value: str) -> str:
    return {"high": "높음", "medium": "중간", "low": "낮음"}.get(
        value.lower(), value or "확인되지 않음"
    )


def verification(status: str) -> str:
    return {
        "PASSED": "근거 검증 통과",
        "MISMATCH": "근거 불일치",
        "NOT_VERIFIED": "미검증",
    }.get(status, "미검증")


def render_layout(
    report: IncidentAnalysisReport,
    now: datetime,
    *,
    gaps: tuple[str, ...],
    analysis_failed: bool,
    css: str,
) -> str:
    obs = report.observations
    narrative = report.narrative
    renderer = CitationRenderer()
    level, reasons = observed_severity(obs)
    warnings = []
    if analysis_failed:
        warnings.append(
            "이 진단은 분석에 실패했다. 결론을 신뢰할 수 없다. 확보된 관측값과 근거를 아래에 표시한다."
        )
    if report.verification_status != "PASSED":
        warnings.append(
            verification(report.verification_status)
            + " · 모델 해석과 원인을 확정된 사실로 읽지 않는다."
        )
    if any(row.failed for row in obs.timeline):
        warnings.append("이 리포트에는 분석하지 못한 구간이 있다.")
    warnings.extend(gaps)
    warnings.extend(report.verification_issues)
    warnings = list(dict.fromkeys(warnings))
    banner = (
        '<aside class="banner" role="note"><b>분석 주의 사항</b>'
        + bullet_list(warnings)
        + "</aside>"
        if warnings
        else ""
    )

    windows = obs.requested or (
        ((report.analyzed_from, report.analyzed_to),)
        if report.analyzed_from and report.analyzed_to
        else ()
    )
    span = (
        " · ".join(f"{kst_stamp(start)} ~ {kst_stamp(end)}" for start, end in windows)
        or "분석 구간 미확인"
    )
    first_cause = narrative.causes[0] if narrative and narrative.causes else None
    cause = (
        first_cause.statement
        if first_cause
        else (narrative.root_cause if narrative else "")
    )
    headline = (
        narrative.headline
        if narrative and narrative.headline
        else "결론이 확인되지 않음"
    )
    actions = narrative.recommendations[:3] if narrative else ()
    summary = (
        '<section id="summary" class="incident-summary"><p class="section-label">핵심 요약</p>'
        f'<h2 class="headline">{esc(headline)}</h2>'
        '<div class="summary-status">'
        f'<span class="sev sev-{(level or "Info").lower()}">관측 심각도 {esc(level or "이상 신호 없음")}</span>'
        f"<span>원인 확신도 {esc(confidence(first_cause.confidence if first_cause else ''))}</span>"
        f"<span>{esc(verification(report.verification_status))}</span></div>"
        f'<p class="cause-summary"><b>유력 원인</b> {esc(cause or "확인되지 않음")}</p>'
        + (
            f'<p class="hint">관측 근거: {esc(", ".join(reasons))}</p>'
            if reasons
            else ""
        )
        + '<div class="next-actions"><h3>우선 확인할 것</h3>'
        + (
            bullet_list(actions)
            if actions
            else '<p class="hint">권장 조치가 확인되지 않음</p>'
        )
        + "</div></section>"
    )

    cards = []
    for card in projected_timeline(report):
        nodes = tuple(
            dict.fromkeys(
                c.evidence.node_name or c.evidence.node_id
                for c in card.evidence_citations
                if c.evidence and (c.evidence.node_name or c.evidence.node_id)
            )
        )
        observations = tuple(
            dict.fromkeys(item.text for item in (*card.impacts, *card.causes))
        )
        local_evidence = tuple(
            c.evidence
            for c in card.evidence_citations
            if c.evidence
            and card.start
            <= c.evidence.event_time.replace(second=0, microsecond=0)
            <= card.end
        )
        if not observations:
            observations = tuple(dict.fromkeys(e.message for e in local_evidence))
        observed = bullet_list(list(observations)) if observations else ""
        interpretations = list(card.interpretations)
        linked_causes = []
        if narrative and report.verification_status == "PASSED":
            refs = {e.evidence_id for e in local_evidence}
            linked_causes = [
                c
                for c in narrative.causes
                if refs.intersection(item.evidence_id for item in c.supporting)
            ]
        interpretation = (
            bullet_list([item.text for item in interpretations])
            if interpretations
            else '<p class="hint">이 시점의 원인 해석은 추가 확인이 필요합니다.</p>'
        )
        if linked_causes:
            interpretation += (
                '<p class="hypothesis-label">연결된 원인 후보</p>'
                + bullet_list(
                    [
                        f"{c.statement} · 확신도 {confidence(c.confidence)}"
                        for c in linked_causes
                    ]
                )
            )
        raw_observations = (
            '<details class="timeline-observations"><summary>해당 구간 분 단위 관측값</summary>'
            + pre("\n".join(timeline_line(row) for row in card.raw_rows))
            + "</details>"
            if card.raw_rows
            else ""
        )
        # All references remain addressable, including those outside the three previews.
        originals = renderer.render(
            card.evidence_citations[:3], compact=True, event_start=card.start
        )
        if len(card.evidence_citations) > 3:
            originals += (
                '<details class="source-details"><summary>반복·추가 근거 '
                + str(len(card.evidence_citations) - 3)
                + "건</summary>"
                + renderer.render(
                    card.evidence_citations[3:], compact=True, event_start=card.start
                )
                + "</details>"
            )
        title = card.representative_event.split(" · 회복 관측:")[0].split(
            " · 구간 종료"
        )[0]
        if interpretations and card.start == card.end:
            title = interpretations[0].text.split(" · ")[0]
        title = title.split(" — ")[0]
        if len(title) > 100:
            title = title[:100] + "…"
        cards.append(
            f'<article class="timeline-card timeline-card-{card.severity.lower()}">'
            f'<div class="event-time"><time>{esc(kst_stamp(card.start))}</time>'
            + (
                f'<span class="event-end">~ {esc(kst_stamp(card.end))}</span>'
                if card.end != card.start
                else ""
            )
            + '</div><div class="event-body"><div class="event-heading">'
            + f'<h3>{esc(title)}</h3><span class="sev sev-{card.severity.lower()}">{esc(card.severity)}</span>'
            + "".join(f'<span class="node-badge">{esc(node)}</span>' for node in nodes)
            + '</div><div class="event-observation"><p class="section-label">관측</p>'
            + observed
            + "</div>"
            + '<div class="event-evidence">'
            + originals
            + "</div>"
            + '<div class="event-interpretation"><p class="section-label">해석 · 의심</p>'
            + interpretation
            + "</div>"
            + raw_observations
            + "</div></article>"
        )
    timeline = (
        '<section id="timeline"><h2>사건 흐름</h2>'
        '<p class="hint">로그 시각 순서 · 반복 신호는 첫 시각과 마지막 시각을 표시합니다. 시간 순서만으로 인과관계를 확정하지 않습니다.</p>'
        '<div class="incident-timeline">'
        + ("".join(cards) or '<p class="hint">표시할 사건이 확인되지 않음</p>')
        + "</div></section>"
    )

    cause_blocks = []
    if narrative:
        for c in narrative.causes:
            cause_blocks.append(
                f'<article class="cause-assessment"><h3>{esc(c.statement or "원인 미확인")}</h3><p>확신도 {esc(confidence(c.confidence))}</p>'
                "<p><b>지지 근거</b> "
                + (
                    ref_links(tuple(item.evidence_id for item in c.supporting))
                    or "확인되지 않음"
                )
                + "</p>"
                + renderer.render_unseen(c.supporting)
                + "<p><b>반박 근거</b> "
                + (
                    ref_links(tuple(item.evidence_id for item in c.contradicting))
                    or "제시된 반박 근거 없음"
                )
                + "</p>"
                + renderer.render_unseen(c.contradicting)
                + "</article>"
            )
        if not narrative.causes and narrative.root_cause:
            cause_blocks.append(
                f"<p>{esc(narrative.root_cause)}</p>"
                + bullet_list([*narrative.supporting, *narrative.contradicting])
            )
        if narrative.unverified:
            cause_blocks.append(
                "<h3>확인하지 못한 것</h3>" + bullet_list(narrative.unverified)
            )
        for f in narrative.findings:
            cause_blocks.append(
                f'<article class="cause-assessment"><h3>{esc(f.title)}</h3><p>{esc(f.detail)}</p>'
                + renderer.render_unseen(f.citations)
                + (bullet_list(f.evidence) if not f.citations else "")
                + "</article>"
            )
        if narrative.recommendations:
            cause_blocks.append(
                "<h3>전체 권장 조치</h3>" + bullet_list(narrative.recommendations)
            )
    causes = (
        '<section id="causes"><h2>원인 판단</h2>'
        + ("".join(cause_blocks) or '<p class="hint">원인 판단이 확인되지 않음</p>')
        + "</section>"
    )

    picks = (
        {p.candidate_id: p.reason for p in narrative.suspect_picks} if narrative else {}
    )
    candidate_lines = []
    for candidate in obs.candidates:
        candidate_lines += [
            candidate_line(candidate),
            *candidate_details(candidate, picks.get(candidate.candidate_id, "")),
        ]
    detail_blocks = [
        ("전체 노드 지표 · 노드별 구간 최대값 (관측값)", node_lines(obs.nodes)),
        ("느린 요청 후보 (관측값 + 모델 선정)", candidate_lines),
        (
            "마스터 노드 로그 (관측값)",
            master_log_lines(obs.master_events, obs.master_log_total),
        ),
        ("가해자 집계 (관측값)", offender_lines(obs.candidates)),
        ("전체 분 단위 관측값", [timeline_line(row) for row in obs.timeline]),
    ]
    details = "".join(
        '<details class="detail-group"><summary>'
        + esc(label)
        + "</summary>"
        + pre("\n".join(lines))
        + "</details>"
        for label, lines in detail_blocks
        if lines
    )
    remaining = citations(
        tuple(
            e.evidence_id for e in report.evidence if e.evidence_id not in renderer.seen
        ),
        report.evidence,
    )
    if remaining:
        details += (
            '<details class="detail-group"><summary>추가 수집 근거 · 출처와 원문</summary>'
            + renderer.render(remaining)
            + "</details>"
        )
    if report.narrative_text:
        details += (
            '<details class="detail-group"><summary>모델 리포트 (평문)</summary>'
            + pre(report.narrative_text)
            + "</details>"
        )
    details = (
        '<section id="details"><h2>상세 자료</h2>'
        + (details or '<p class="hint">추가 자료 없음</p>')
        + "</section>"
    )
    limits = '<section id="limits"><h2>분석 범위와 한계</h2>' + bullet_list(
        overview_lines(obs)
    )
    if warnings:
        limits += bullet_list(warnings)
    health = health_lines(obs.health, obs.requested)
    if health:
        limits += (
            '<details class="detail-group"><summary>참고: 클러스터 현재 상태 (사고 시각 상태 아님)</summary>'
            + pre("\n".join(health))
            + "</details>"
        )
    limits += "</section>"
    return (
        '<!doctype html><html lang="ko"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"<title>ClusterDoctor 진단 리포트 {esc(kst_stamp(now))}</title><style>{css}\n{LAYOUT_CSS}\n{QUERY_CSS}</style></head>"
        '<body><main class="wrap"><header><p class="eyebrow">ClusterDoctor / Incident report</p>'
        f'<h1>{esc(report.cluster or "Elasticsearch")} · 장애 진단</h1><p class="stamp">{esc(span)}</p>'
        f'<p class="hint">시각 기준 {esc(obs.time_basis or "미확인")} · 생성 {esc(kst_stamp(now))}</p></header>'
        + banner
        + summary
        + '<nav class="report-nav" aria-label="리포트 목차"><a href="#summary">요약</a><a href="#timeline">타임라인</a><a href="#query-ranking">검색 요청</a><a href="#causes">원인 판단</a><a href="#details">상세 자료</a><a href="#limits">범위와 한계</a></nav>'
        + timeline
        + render_query_ranking(obs.query_requests)
        + causes
        + details
        + limits
        + '<details class="source"><summary>리포트 평문 (관측값 + 모델 판단)</summary>'
        + pre(render_text(report))
        + "</details>"
        "</main></body></html>"
    )


LAYOUT_CSS = """
.wrap{max-width:900px}header .hint{margin:0}.incident-summary{padding:26px 0 22px;
border-bottom:1px solid var(--line)}.section-label{font-size:12px;color:var(--ink-2);margin:0 0 8px}
.incident-summary .headline{font-size:clamp(22px,3vw,30px);border:0;padding:0;line-height:1.4}
.summary-status{display:flex;flex-wrap:wrap;gap:10px 20px;align-items:center;margin:16px 0;
font-size:13px;color:var(--ink-2)}.cause-summary{margin-top:12px}.next-actions{margin-top:20px}
.next-actions h3{font-size:15px;margin:0 0 8px}.next-actions ul{margin:0;padding-left:22px}
.report-nav{display:flex;flex-wrap:wrap;gap:12px 22px;padding:18px 0;border-bottom:1px solid var(--line)}
a{color:var(--accent-ink);text-underline-offset:3px}.report-nav a{font-size:13px;text-decoration:none}
.timeline-card{background:transparent;border:0;border-radius:0;border-bottom:1px solid var(--line);
padding:12px 0 24px;break-inside:auto}.timeline-card::before{top:19px}
.timeline-card dl{grid-template-columns:1fr;gap:4px}.timeline-card dd{margin-bottom:10px}
.timeline-card dd li{display:block}.timeline-card dd li::before{display:none}
.timeline-refs{display:inline-block;font-size:12px;margin-left:6px}
.timeline-evidence{margin-top:20px}.evidence-block{margin:14px 0 20px;scroll-margin-top:20px}
.evidence-title{font-size:13px;font-weight:600;margin:0 0 5px}.evidence-meta{font-size:12px;
color:var(--ink-2);overflow-wrap:anywhere}.raw-label{font-size:12px;color:var(--ink-2);margin:10px 0 6px}
pre.raw{border:0;border-left:2px solid var(--line-strong);border-radius:0;white-space:pre-wrap;
overflow-wrap:anywhere;word-break:break-word;overflow-x:visible}pre.raw code{font:inherit}
.log-remainder{margin-top:-4px}.log-remainder summary,.detail-group summary{cursor:pointer;
font-size:13px;color:var(--ink-2);padding:10px 0}.detail-group{border-bottom:1px solid var(--line)}
.cause-assessment{padding:18px 0;border-bottom:1px solid var(--line)}
.cause-assessment h3{font-size:16px;margin:0 0 8px}.cause-assessment p{overflow-wrap:anywhere}
.stamp,.headline,h1,.hint,li{overflow-wrap:anywhere}.sev{font-size:12px;letter-spacing:0}
@media(max-width:640px){.summary-status{gap:8px 12px}.timeline-card{padding:10px 0 20px}
.evidence-meta{font-size:12px}.report-nav{gap:10px 18px}}
@media print{.wrap{max-width:none;padding:0}.report-nav,details.source{display:none}
.timeline-card,.evidence-block{break-inside:auto}.evidence-title{break-after:avoid}
details.detail-group>summary{display:none}details.detail-group>:not(summary){display:block}
details.log-remainder>:not(summary){display:block}.log-remainder summary{display:none}
pre.raw{background:#f4f4f4;color:#111}.banner{break-inside:avoid}}
"""

LAYOUT_CSS += """
.wrap{max-width:1120px}.incident-timeline{padding:0;gap:0}.incident-timeline::before{left:175px;top:22px;bottom:22px}
.timeline-card{display:grid;grid-template-columns:150px minmax(0,1fr);gap:52px;padding:28px 0;border:0}
.timeline-card::before{left:170px;top:34px;background:var(--ground)}
.event-time{font:12px var(--mono);color:var(--ink-2);padding-top:3px;line-height:1.8}
.event-time time{display:block;font-weight:600;color:var(--ink)}.event-end{display:block}
.event-body{min-width:0;border-bottom:1px solid var(--line);padding-bottom:28px}
.event-heading{display:flex;align-items:center;flex-wrap:wrap;gap:8px 12px}.event-heading h3{flex-basis:100%;font-size:18px;margin:0 0 2px}
.node-badge{font-size:12px;color:var(--ink-2);background:var(--surface);padding:3px 8px;border-radius:4px}
.event-observation{margin-top:18px}.event-observation ul,.event-interpretation ul{padding-left:20px;margin:6px 0}
.event-observation li,.event-interpretation li{font-size:14px;line-height:1.7}
.event-evidence{margin:16px 0}.compact-evidence{padding:12px 14px;background:var(--surface);margin:10px 0;border-radius:6px}
.source-location{font-size:12px;color:var(--ink-2);overflow-wrap:anywhere;margin:3px 0}
.compact-evidence pre.raw{margin:6px 0;padding:10px 12px;font-size:12px;background:transparent}
.source-details summary,.timeline-observations summary{font-size:12px;cursor:pointer;color:var(--ink-2);padding:8px 0}
.source-details .evidence-meta{padding:10px 0}.event-interpretation{border-left:2px solid var(--accent-ink);padding:2px 0 2px 14px;margin-top:18px}
.hypothesis-label{font-size:12px;color:var(--ink-2);margin:12px 0 0}
@media(max-width:700px){.incident-timeline{padding-left:20px}.incident-timeline::before{left:4px}
.timeline-card{display:block;padding:20px 0}.timeline-card::before{left:-20px;top:27px}
.event-time{margin-bottom:10px}.event-end{display:inline}.event-heading h3{font-size:16px}.compact-evidence{padding:10px}}
@media print{.source-details::details-content{content-visibility:visible;display:block}.source-details>summary{display:none}
.timeline-card{display:block}.incident-timeline::before,.timeline-card::before{display:none}.event-time{margin-bottom:8px}}
"""
