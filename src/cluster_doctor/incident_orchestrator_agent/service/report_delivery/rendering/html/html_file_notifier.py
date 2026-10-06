"""Publish standalone operator reports with a plain-text fallback.

No external assets are required. Publication failures retain the plain-text
report in the application log. Page layout lives in report_layout; HTML escaping in evidence_link.
"""

from __future__ import annotations

from cluster_doctor.incident_orchestrator_agent.service.report_delivery.projection.report_content import is_validation_diagnostic

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cluster_doctor.incident_analysis_agent.agent.runtime.pseudonym import (
    unrestored_aliases,
)
from cluster_doctor.incident_orchestrator_agent.model.incident_report import (
    IncidentAnalysisReport,
)
from cluster_doctor.incident_orchestrator_agent.model.report_delivery import (
    ReportPublication,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.publication.file.report_publisher import (
    ReportPublisher,
)
from cluster_doctor.incident_orchestrator_agent.service.report_delivery.rendering.text.report_text import (
    render_text,
    scrub,
)

_logger = logging.getLogger(__name__)

_KST = timezone(timedelta(hours=9))

_FILENAME_FORMAT = "report-%Y%m%d-%H%M%S"

_NON_VISIBLE = re.compile(r"<(script|style)[^>]*>.*?</>", re.DOTALL | re.IGNORECASE)
_TAG = re.compile(r"<[^>]+>")


def _warn_unrestored_aliases(html: str) -> None:
    """저장 직전 본문에 가명이 남았는지 본다. 내용은 바꾸지 않고 로그만 남긴다.

    가명은 LLM 경계 안쪽에만 있어야 하고 응답에서 복원된다. 모델이 가명을
    변형해 쓰면 복원이 비껴가므로, 어떤 값이 샜는지 알 수 있게 가명 문자열만
    기록한다. 문장은 실제 값을 담고 있으므로 로그에 싣지 않는다.
    """
    visible = _TAG.sub(" ", _NON_VISIBLE.sub(" ", html))
    leftover = unrestored_aliases(visible)
    if leftover:
        _logger.warning("리포트에 복원되지 않은 가명이 남았다: %s", ", ".join(leftover))


class HtmlFileReportPublisher(ReportPublisher):
    """리포트를 ``output_dir`` 아래 HTML 파일 한 건으로 저장한다.

    디렉터리는 생성자가 아니라 첫 저장 시점에 만든다. 생성자에서 만들면
    ``build_trigger_service``를 부르는 것만으로 디렉터리가 생겨, 의존성 조립을
    검증하는 테스트가 작업 디렉터리에 흔적을 남긴다.
    """

    def __init__(self, output_dir: str | Path = "reports") -> None:
        self._output_dir = Path(output_dir)

    async def publish(
        self,
        report: IncidentAnalysisReport,
        *,
        gaps: tuple[str, ...] = (),
        analysis_failed: bool = False,
    ) -> ReportPublication:
        # HTML 문자열은 evidence_link.esc에서 이스케이프·인코딩 정리한다.
        # 평문 로그 폴백에는 scrub을 별도로 적용한다.
        gaps = tuple(scrub(gap) for gap in gaps)
        for gap in gaps:
            if is_validation_diagnostic(gap):
                _logger.warning("%s", gap)

        # 파일 쓰기는 짧지만 이벤트 루프에서 하지 않는다. 같은 루프가 Kafka를
        # 계속 소비하고 있고, 리포트는 수십 KB까지 자란다.
        try:
            text_length = len(render_text(report, analysis_failed=analysis_failed))
            path = await asyncio.to_thread(self._write, report, gaps, analysis_failed)
        except Exception as exc:  # noqa: BLE001
            # 파일·렌더링·인코딩 오류가 publication 밖으로 새어 나가지 않게 하고
            # 확보한 진단을 잃지 않도록 평문 로그로 대체한다.
            _logger.error("리포트 HTML 저장 실패(%s) — 전문을 로그로 남긴다", exc)
            try:
                _logger.info("\n%s", scrub(render_text(report, analysis_failed=analysis_failed)))
            except Exception:
                # 렌더링 자체가 실패한 경우다. 그때도 이 폴백이 죽으면 안 된다.
                _logger.exception("리포트 평문 렌더링도 실패했다")
            return ReportPublication(text_length=locals().get("text_length", 0))

        _logger.info("리포트 저장: %s", path)
        return ReportPublication(text_length=text_length)

    def _write(
        self,
        report: IncidentAnalysisReport,
        gaps: tuple[str, ...] = (),
        analysis_failed: bool = False,
    ) -> Path:
        self._output_dir.mkdir(parents=True, exist_ok=True)
        now = datetime.now(_KST)
        path = _unique_path(self._output_dir, now)
        html = render_report(
            report,
            generated_at=now,
            gaps=gaps,
            analysis_failed=analysis_failed,
        )
        _warn_unrestored_aliases(html)
        path.write_text(html, encoding="utf-8")
        return path


def _unique_path(output_dir: Path, now: datetime) -> Path:
    """같은 초에 두 건이 저장되어도 앞의 것을 덮지 않게 한다.

    재트리거는 10초 간격이라 부딪히지 않지만, 인스턴스를 둘 띄우면 같은 초에
    저장될 수 있다. 파일명에 콜론을 쓰지 않는 것도 의도적이다 — Windows에서
    쓸 수 없는 문자다.
    """
    stem = now.strftime(_FILENAME_FORMAT)
    path = output_dir / f"{stem}.html"
    suffix = 2
    while path.exists():
        path = output_dir / f"{stem}-{suffix}.html"
        suffix += 1
    return path


def render_report(
    report: IncidentAnalysisReport,
    generated_at: datetime | None = None,
    gaps: tuple[str, ...] = (),
    analysis_failed: bool = False,
) -> str:
    """리포트를 완결된 HTML 문서 한 장으로 만든다.

    ``generated_at``은 테스트가 시각을 고정할 수 있게 열어 뒀다.

    ``gaps``와 ``analysis_failed``는 별도 안내 영역으로 그린다. 모델이 쓴 본문에 섞지
    않는 이유는 두 가지다 — 본문과 시스템이 덧붙인 사실이 구별되어야 하고,
    모델이 프롬프트를 어겨 누락을 밝히지 않았더라도 이 안내는 반드시 남는다.
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

