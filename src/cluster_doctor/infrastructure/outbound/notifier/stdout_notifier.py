import logging

from cluster_doctor.application.port.outbound.notifier import Notifier

_logger = logging.getLogger(__name__)


class StdoutNotifier(Notifier):
    """리포트를 로그로만 내보낸다.

    배선에서는 ``HtmlFileNotifier``가 쓰인다. 이쪽은 파일을 남길 수 없는
    환경(테스트, 임시 실행)을 위한 최소 구현으로 남겨 둔다.
    """

    async def notify(
        self,
        message: str,
        *,
        gaps: tuple[str, ...] = (),
        analysis_failed: bool = False,
    ) -> None:
        if analysis_failed:
            _logger.error("[분석 실패] 아래 리포트의 내용을 신뢰할 수 없다")
        if gaps:
            _logger.warning("수집하지 못한 근거: %s", " / ".join(gaps))
        _logger.info("\n%s", message)
