import logging

from cluster_doctor.application.port.outbound.notifier import Notifier

_logger = logging.getLogger(__name__)


class StdoutNotifier(Notifier):
    async def notify(self, message: str) -> None:
        _logger.info("\n%s", message)
