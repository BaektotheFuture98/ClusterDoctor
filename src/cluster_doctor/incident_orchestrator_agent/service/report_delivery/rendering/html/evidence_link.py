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
