import asyncio
import logging
import os
from pathlib import Path

from cluster_doctor.bootstrap.configuration.settings import LoggingSettings, get_settings
from cluster_doctor.bootstrap.dependency.wiring import (
    build_kafka_consumer,
    build_slowlog_intake,
)
from cluster_doctor.bootstrap.lifecycle.app_lifecycle import close_clickhouse_client
from cluster_doctor.exceptions import KafkaUnavailableError
from cluster_doctor.log_context import IncidentIdLogFilter

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s [%(incident_id)s] %(message)s"
_HANDLER_MARKER = "_cluster_doctor_owned_handler"


def configure_logging() -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    already_configured = any(
        getattr(handler, _HANDLER_MARKER, False) for handler in root_logger.handlers
    )
    if already_configured:
        return

    log_dir = Path(LoggingSettings().log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(_LOG_FORMAT)
    incident_id_filter = IncidentIdLogFilter()

    file_handler = logging.FileHandler(log_dir / "app.log", encoding="utf-8")
    stream_handler = logging.StreamHandler()
    for handler in (file_handler, stream_handler):
        handler.setFormatter(formatter)
        handler.addFilter(incident_id_filter)
        setattr(handler, _HANDLER_MARKER, True)
        root_logger.addHandler(handler)


configure_logging()


async def main() -> None:
    settings = get_settings()
    intake = build_slowlog_intake(settings)
    consumer = build_kafka_consumer(intake, settings)

    try:
        await consumer.run()
    except KafkaUnavailableError as exc:
        logging.getLogger(__name__).critical("%s; terminating process with exit code 1", exc)
        logging.shutdown()
        # Consumer cleanup has already been attempted. Normal shutdown waits
        # for analysis threads, so it cannot enforce this fatal outage policy.
        # Exit the whole process, discarding pending/in-flight analysis.
        os._exit(1)
    finally:
        try:
            await intake.close()
        finally:
            close_clickhouse_client()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
