"""Operator-first layout: conclusion, chronological events, and original evidence."""

from datetime import datetime

from cluster_doctor.incident_analysis_agent.model.observations import observed_severity
from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    IncidentAnalysisReport,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.evidence_citation import (
    kst_stamp,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.assessment_view import (
    render_actions,
    render_causes,
    render_findings,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.evidence_link import (
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
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.evidence_view import (
    render_evidence,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.observation_view import (
    render_metadata,
    render_observation_detail,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import (
    projected_timeline,
)


def bullet_list(items: tuple[str, ...] | list[str]) -> str:
    return "<ul>" + "".join(f"<li>{esc(item)}</li>" for item in items) + "</ul>"


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
    evidence_section = render_evidence(
        report.evidence, render_observation_detail(report)
    )
    metadata = render_metadata(report, now)
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
        '<nav class="report-nav" aria-label="리포트 목차"><a href="#summary">요약</a><a href="#timeline">사건 흐름</a><a href="#causes">원인 판단</a><a href="#query-ranking">의심 요청</a>'
        + ('<a href="#actions">조치</a>' if actions else "")
        + '<a href="#evidence">근거</a></nav>'
        + summary
        + alert
        + findings
        + timeline
        + causes
        + render_query_ranking(obs.query_requests, obs.candidates, picks)
        + actions
        + evidence_section
        + metadata
        + "</main></body></html>"
    )

