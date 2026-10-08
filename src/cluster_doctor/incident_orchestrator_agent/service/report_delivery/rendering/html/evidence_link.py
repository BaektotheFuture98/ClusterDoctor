"""HTML 리포트 섹션들이 공유하는 escape 헬퍼."""

import html

from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import (
    scrub,
)


def esc(value: object) -> str:
    return html.escape(scrub(str(value)), quote=True)

