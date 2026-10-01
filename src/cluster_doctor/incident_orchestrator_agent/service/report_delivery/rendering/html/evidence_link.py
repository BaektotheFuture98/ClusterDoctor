"""Escaping and evidence-anchor helpers shared by the HTML report sections."""

import hashlib
import html

from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import (
    scrub,
)


def esc(value: object) -> str:
    return html.escape(scrub(str(value)), quote=True)


def anchor(ref: str) -> str:
    return (
        "evidence-"
        + hashlib.sha256(ref.encode("utf-8", errors="surrogatepass")).hexdigest()
    )


def evidence_ref(ref: str, known: set[str]) -> str:
    # A ref without collected evidence has no anchor, so it is marked instead of linked.
    if ref in known:
        return f'<a href="#{anchor(ref)}">{esc(ref)}</a>'
    return f'<span class="hint">{esc(ref)} 근거 없음(dangling)</span>'
