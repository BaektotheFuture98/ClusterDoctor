"""Operator-first layout: conclusion, chronological events, and original evidence."""

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
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.assessment_view import (
    render_actions,
    render_causes,
    render_findings,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.evidence_link import (
    anchor,
    esc,
    evidence_ref,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.query_ranking import (
    render_query_ranking,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.summary_view import (
    DEMO_GAP,
    DEMO_NOTE,
    confidence_label,
    format_window,
    is_demo,
    key_observations,
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


def pre(text: str) -> str:
    return f'<pre class="raw"><code>{esc(text)}</code></pre>'


class CitationRenderer:
    def __init__(self):
        self.seen: set[str] = set()

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


def verification(status: str) -> str:
    return {
        "PASSED": "근거 검증 통과",
        "MISMATCH": "근거 불일치",
        "NOT_VERIFIED": "미검증",
    }.get(status, "미검증")


def render_alert(report: IncidentAnalysisReport, notices: list[str]) -> str:
    out = ['<section class="alert-card" role="note">']
    if report.verification_status != "PASSED":
        issues = report.verification_issues
        out.append(
            "<h2>Evidence verification issue</h2>"
            f"<p>{esc(VERIFICATION_NOTE.get(report.verification_status, VERIFICATION_NOTE['NOT_VERIFIED']))}</p>"
        )
        if issues:
            out.append(
                f'<details><summary>{len(issues)} issues found</summary>'
                + bullet_list(issues)
                + "</details>"
            )
    if notices:
        title = "h3" if len(out) > 1 else "h2"
        out.append(f"<{title}>분석 주의 사항</{title}>" + bullet_list(notices))
    out.append("</section>")
    return "".join(out)


VERIFICATION_NOTE = {
    "MISMATCH": "모델이 인용한 근거가 수집된 원문과 일치하지 않습니다. 모델 해석과 원인을 확정된 사실로 읽지 않습니다.",
    "NOT_VERIFIED": "근거 검증이 완료되지 않았습니다. 모델 해석과 원인을 확정된 사실로 읽지 않습니다.",
}


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
    demo = is_demo(gaps)
    notices = []
    if analysis_failed:
        notices.append(
            "이 진단은 분석에 실패했다. 결론을 신뢰할 수 없다. 확보된 관측값과 근거를 아래에 표시한다."
        )
    if any(row.failed for row in obs.timeline):
        notices.append("이 리포트에는 분석하지 못한 구간이 있다.")
    notices.extend(gap for gap in gaps if gap != DEMO_GAP)
    notices = list(dict.fromkeys(notices))
    unverified = report.verification_status != "PASSED"
    warnings = list(
        dict.fromkeys(
            [
                *notices,
                *(
                    [
                        verification(report.verification_status)
                        + " · 모델 해석과 원인을 확정된 사실로 읽지 않는다."
                    ]
                    if unverified
                    else []
                ),
                *report.verification_issues,
            ]
        )
    )
    alert = render_alert(report, notices) if unverified or notices else ""

    windows = obs.requested or (
        ((report.analyzed_from, report.analyzed_to),)
        if report.analyzed_from and report.analyzed_to
        else ()
    )
    span = (
        " · ".join(
            f'<time datetime="{start.isoformat()}/{end.isoformat()}">{esc(format_window(start, end))}</time>'
            for start, end in windows
        )
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
    key_items = key_observations(obs)
    severity = (
        f'<span class="sev sev-{level.lower()}">{esc(level.upper())}</span>'
        if level
        else '<span class="hint">이상 신호 없음</span>'
    )
    summary = (
        '<section id="summary" class="incident-summary">'
        '<p class="section-label">핵심 요약 <span class="model-tag">분석</span></p>'
        f'<h2 class="headline">{esc(headline)}</h2>'
        '<div class="summary-field"><p class="field-label">Observed severity</p>'
        f"<p>{severity}</p>"
        + (
            f'<p class="hint">{esc(", ".join(reasons))}</p>'
            if reasons
            else ""
        )
        + "</div>"
        + (
            '<div class="summary-field"><h3 class="field-label">주요 관측</h3><dl class="key-observations">'
            + "".join(
                f"<div><dt>{esc(k)}</dt><dd>{esc(v)}</dd></div>" for k, v in key_items
            )
            + "</dl></div>"
            if key_items
            else ""
        )
        + '<div class="summary-field cause-summary">'
        '<p class="field-label">유력 원인 <span class="model-tag">판단</span></p>'
        f"<p>{esc(cause or '확인되지 않음')}</p></div>"
        '<div class="summary-field"><p class="field-label">Root cause confidence <span class="model-tag">판단</span></p>'
        f'<p class="confidence-value">{esc(confidence_label(first_cause.confidence if first_cause else ""))}</p>'
        "</div></section>"
    )

    evidence_ids = {e.evidence_id for e in report.evidence}

    cards = []
    timeline_cards = projected_timeline(report)
    for card in timeline_cards:
        nodes = tuple(
            dict.fromkeys(
                c.evidence.node_name or c.evidence.node_id
                for c in card.evidence_citations
                if c.evidence and (c.evidence.node_name or c.evidence.node_id)
            )
        )
        observations = tuple(
            dict.fromkeys(item.text for item in (*card.impacts, *card.causes))
        ) or tuple(
            dict.fromkeys(
                c.evidence.message for c in card.evidence_citations if c.evidence
            )
        )
        interpretations = card.interpretations
        analysis = (
            interpretations
            if narrative and report.verification_status == "PASSED"
            else ()
        )
        title = card.representative_event.split(" · 회복 관측:")[0].split(
            " · 구간 종료"
        )[0]
        if interpretations and card.start == card.end:
            title = interpretations[0].text.split(" · ")[0]
        title = title.split(" — ")[0]
        if len(title) > 100:
            title = title[:100] + "…"
        severity_class = card.severity.lower()
        cards.append(
            f'<article class="timeline-event timeline-event-{severity_class}">'
            f'<div class="event-time"><time datetime="{card.start.isoformat()}">{esc(kst_stamp(card.start))}</time>'
            + (
                f'<time datetime="{card.end.isoformat()}">~ {esc(kst_stamp(card.end))}</time>'
                if card.end != card.start
                else ""
            )
            + '</div><div class="event-body"><p class="event-meta">'
            + (
                f'<span class="sev sev-{severity_class}">{esc(card.severity.upper())}</span>'
                if card.severity
                else ""
            )
            + (f'<span class="event-node">{esc(", ".join(nodes))}</span>' if nodes else "")
            + f"</p><h3>{esc(title)}</h3>"
            + (
                f'<p class="event-observation">{esc(observations[0])}</p>'
                if observations
                else ""
            )
            + (
                '<p class="analysis-note"><span class="model-tag">분석</span> '
                + esc(" · ".join(item.text for item in analysis))
                + "</p>"
                if analysis
                else ""
            )
            + (
                f'<p class="event-refs">{" ".join(evidence_ref(ref, evidence_ids) for ref in dict.fromkeys(card.evidence_refs))}</p>'
                if card.evidence_refs
                else ""
            )
            + "</div></article>"
        )
    timeline = (
        '<section id="timeline"><h2>사건 흐름</h2>'
        '<p class="hint">로그 시각 순서 · 반복 신호는 첫 시각과 마지막 시각을 표시합니다. 시간 순서만으로 인과관계를 확정하지 않습니다.</p>'
        '<div class="incident-timeline">'
        + ("".join(cards) or '<p class="hint">표시할 사건이 확인되지 않음</p>')
        + "</div></section>"
    )

    causes = render_causes(narrative, evidence_ids)
    findings = render_findings(
        narrative,
        {ref for card in timeline_cards for ref in card.evidence_refs},
        evidence_ids,
    )
    actions = render_actions(narrative)

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
    # Renders every evidence item so each link in the sections above resolves.
    remaining = citations(
        tuple(e.evidence_id for e in report.evidence), report.evidence
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
        f"<title>ClusterDoctor 진단 리포트 {esc(kst_stamp(now))}</title><style>{css}</style></head>"
        '<body><main class="wrap"><header>'
        '<div class="header-top"><p class="eyebrow">ClusterDoctor · Incident Report</p>'
        + ('<span class="demo-badge">DEMO DATA</span>' if demo else "")
        + "</div>"
        + f'<h1>{esc(report.cluster or "Elasticsearch")}</h1><p class="stamp">{span}</p>'
        + f'<p class="hint">Generated {esc(kst_stamp(now))} · Time basis: {esc(obs.time_basis or "미확인")}</p>'
        + (f'<p class="hint">{esc(DEMO_NOTE)}</p>' if demo else "")
        + "</header>"
        '<nav class="report-nav" aria-label="리포트 목차"><a href="#summary">요약</a><a href="#timeline">사건 흐름</a><a href="#causes">원인 판단</a><a href="#query-ranking">의심 요청</a><a href="#actions">조치</a><a href="#evidence">근거</a></nav>'
        + summary
        + alert
        + findings
        + timeline
        + causes
        + render_query_ranking(obs.query_requests, obs.candidates, picks)
        + actions
        + details
        + limits
        + '<details class="source"><summary>리포트 평문 (관측값 + 모델 판단)</summary>'
        + pre(render_text(report))
        + "</details>"
        "</main></body></html>"
    )

