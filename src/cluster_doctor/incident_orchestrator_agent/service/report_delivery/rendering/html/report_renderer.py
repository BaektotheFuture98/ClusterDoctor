"""운영자 리포트를 파일 저장 없이 완결된 HTML 문서로 렌더링한다."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    IncidentAnalysisReport,
)

_KST = timezone(timedelta(hours=9))


def render_report(
    report: IncidentAnalysisReport,
    generated_at: datetime | None = None,
    gaps: tuple[str, ...] = (),
    analysis_failed: bool = False,
) -> str:
    """리포트를 완결된 HTML 문서 한 장으로 만든다.

    ``generated_at``은 테스트가 시각을 고정할 수 있게 열어 뒀다.

    ``gaps``는 "수집 데이터 보기" 맨 아래 "수집·분석 누락"에, ``analysis_failed``는
    사건 분석 안내로 그린다. 모델이 쓴 본문에 섞지 않는 이유는 두 가지다 — 본문과
    시스템이 덧붙인 사실이 구별되어야 하고, 모델이 프롬프트를 어겨 누락을 밝히지
    않았더라도 이 안내는 반드시 남는다. 검증 내부 사유는 싣지 않는다.
    """
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.report_layout import (
        render_layout,
    )
    from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.html.report_style import (
        REPORT_CSS,
    )

    return render_layout(
        report,
        generated_at or datetime.now(_KST),
        gaps=gaps,
        analysis_failed=analysis_failed,
        css=REPORT_CSS,
    )
