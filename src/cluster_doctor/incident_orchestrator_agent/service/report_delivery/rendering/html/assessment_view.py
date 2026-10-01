"""Cause assessment, key findings and recommended actions."""

from cluster_doctor.incident_orchestrator_agent.model.evidence_citation import (
    EvidenceCitation,
)
from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    CauseAssessment,
    Finding,
    Narrative,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.evidence_link import (
    esc,
    evidence_ref,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.summary_view import (
    confidence_label,
)

MAX_FINDINGS = 4
NO_CONTRADICTION = "현재 수집된 근거에서 명시적인 반증은 확인되지 않음"
SEVERITY_CLASSES = {"critical", "warning", "info"}


def marked_list(items: tuple[str, ...], markers: list[str]) -> str:
    return (
        '<ol class="marked-list">'
        + "".join(
            f'<li><span class="marker">{marker}</span><span>{esc(item)}</span></li>'
            for marker, item in zip(markers, items)
        )
        + "</ol>"
    )


def numbered_list(items: tuple[str, ...]) -> str:
    return marked_list(items, [f"{i:02d}" for i in range(1, len(items) + 1)])


def evidence_lines(items: tuple[EvidenceCitation, ...], known: set[str]) -> str:
    if not items:
        return '<p class="hint">—</p>'
    lines = []
    for item in items:
        e = item.evidence
        message = e.message.splitlines()[0] if e and e.message else ""
        lines.append(
            f'<li class="evidence-line">{evidence_ref(item.evidence_id, known)}'
            + (f"<span>{esc(message)}</span>" if message else "")
            + "</li>"
        )
    return '<ul class="evidence-lines">' + "".join(lines) + "</ul>"


def text_lines(items: tuple[str, ...]) -> str:
    if not items:
        return '<p class="hint">—</p>'
    return (
        '<ul class="evidence-lines">'
        + "".join(f'<li class="evidence-line"><span>{esc(i)}</span></li>' for i in items)
        + "</ul>"
    )


def cause_article(
    statement: str, confidence: str, supporting: str, contradicting: str, has_counter: bool
) -> str:
    return (
        '<article class="cause-assessment">'
        f"<h3>{esc(statement or '원인 미확인')}</h3>"
        '<p class="cause-confidence"><span class="field-label">Confidence</span>'
        f'<strong class="confidence-value">{esc(confidence_label(confidence))}</strong>'
        '<span class="model-tag">판단</span></p>'
        f'<div class="cause-evidence"><h4>판단 근거</h4>{supporting}</div>'
        '<div class="cause-evidence"><h4>반증</h4>'
        + (contradicting if has_counter else f'<p class="hint">{NO_CONTRADICTION}</p>')
        + "</div></article>"
    )


def render_causes(narrative: Narrative | None, known: set[str]) -> str:
    blocks = []
    if narrative:
        for c in narrative.causes:
            blocks.append(cause_from(c, known))
        if not narrative.causes and narrative.root_cause:
            blocks.append(
                cause_article(
                    narrative.root_cause,
                    "",
                    text_lines(narrative.supporting),
                    text_lines(narrative.contradicting),
                    bool(narrative.contradicting),
                )
            )
        if narrative.unverified:
            markers = (
                ["?"]
                if len(narrative.unverified) == 1
                else [f"{i:02d}" for i in range(1, len(narrative.unverified) + 1)]
            )
            blocks.append(
                '<div class="unverified"><h3>미확인 사항 <span class="model-tag">판단</span></h3>'
                + marked_list(narrative.unverified, markers)
                + "</div>"
            )
    return (
        '<section id="causes"><h2>원인 판단</h2>'
        + ("".join(blocks) or '<p class="hint">원인 판단이 확인되지 않음</p>')
        + "</section>"
    )


def cause_from(c: CauseAssessment, known: set[str]) -> str:
    return cause_article(
        c.statement,
        c.confidence,
        evidence_lines(c.supporting, known),
        evidence_lines(c.contradicting, known),
        bool(c.contradicting),
    )


def finding_node(f: Finding) -> str:
    names = dict.fromkeys(
        c.evidence.node_name or c.evidence.node_id
        for c in f.citations
        if c.evidence and (c.evidence.node_name or c.evidence.node_id)
    )
    return ", ".join(names)


def render_findings(
    narrative: Narrative | None, timeline_refs: set[str], known: set[str]
) -> str:
    """Findings the timeline does not already show; one with no refs cannot be matched."""
    if not narrative:
        return ""
    shown = [
        f
        for f in narrative.findings
        if not f.citations
        or not all(c.evidence_id in timeline_refs for c in f.citations)
    ][:MAX_FINDINGS]
    if not shown:
        return ""
    lines = []
    for f in shown:
        severity = f.severity.lower()
        sev_class = f" sev-{severity}" if severity in SEVERITY_CLASSES else ""
        node = finding_node(f)
        refs = " ".join(evidence_ref(c.evidence_id, known) for c in f.citations)
        lines.append(
            '<li class="finding-line"><p class="finding-head">'
            + (
                f'<span class="sev{sev_class}">{esc(f.severity.upper())}</span>'
                if f.severity
                else '<span class="hint">분류 없음</span>'
            )
            + f'<span class="finding-title">{esc(f.title)}</span>'
            + (f'<span class="event-node">{esc(node)}</span>' if node else "")
            + (f'<span class="event-refs">{refs}</span>' if refs else "")
            + "</p>"
            + (f'<p class="hint">{esc(f.detail)}</p>' if f.detail else "")
            + "</li>"
        )
    return (
        '<section id="findings"><h2>주요 발견 <span class="model-tag">분석</span></h2>'
        '<ul class="finding-lines">' + "".join(lines) + "</ul></section>"
    )


def render_actions(narrative: Narrative | None) -> str:
    if not narrative or not narrative.recommendations:
        return ""
    return (
        '<section id="actions"><h2>권장 조치 <span class="model-tag">판단</span></h2>'
        + numbered_list(narrative.recommendations)
        + "</section>"
    )
