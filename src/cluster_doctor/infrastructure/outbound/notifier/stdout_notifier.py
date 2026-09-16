import logging

from cluster_doctor.application.port.outbound.notifier import Notifier
from cluster_doctor.domain.model.diagnosis_report import DiagnosisReport
from cluster_doctor.infrastructure.outbound.notifier.report_text import (
    render_text,
    scrub,
)

_logger = logging.getLogger(__name__)


class StdoutNotifier(Notifier):
    """리포트를 로그로만 내보낸다.

    배선에서는 ``HtmlFileNotifier``가 쓰인다. 이쪽은 파일을 남길 수 없는
    환경(테스트, 임시 실행)을 위한 최소 구현으로 남겨 둔다.
    """

    async def notify(
        self,
        report: DiagnosisReport,
        *,
        gaps: tuple[str, ...] = (),
        analysis_failed: bool = False,
    ) -> None:
        if analysis_failed:
            _logger.error("[분석 실패] 아래 리포트의 내용을 신뢰할 수 없다")
        if gaps:
            _logger.warning("수집하지 못한 근거: %s", " / ".join(gaps))
        # HTML 쪽과 같은 render_text를 쓴다. 두 notifier가 각자 그리면 같은
        # 관측값이 화면마다 다르게 보인다.
        #
        # 렌더링이 notify 안에서 일어나므로 실패 갈래가 있다. 짝 없는
        # 서로게이트가 섞이면 로그 핸들러의 인코딩이 터지고, 그 예외가 새면
        # _run_agent의 succeeded가 False로 남아 리포트도 잃고 재트리거까지
        # 막힌다 — HtmlFileNotifier가 통째로 코드를 들여 막는 그 사고다.
        try:
            _logger.info("\n%s", scrub(render_text(report)))
        except Exception:  # noqa: BLE001
            _logger.exception("리포트 평문 렌더링 실패 — 관측값만 남긴다")
            _logger.info("관측값: %r", report.observations)
