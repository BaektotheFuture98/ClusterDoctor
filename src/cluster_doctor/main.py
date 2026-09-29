import asyncio
import logging
import os

from cluster_doctor.bootstrap.configuration.settings import get_settings
from cluster_doctor.bootstrap.dependency.wiring import (
    build_kafka_consumer,
    build_slowlog_intake,
)
from cluster_doctor.bootstrap.lifecycle.app_lifecycle import close_clickhouse_client

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
_HANDLER_MARKER = "_cluster_doctor_owned_handler"


def configure_logging() -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    already_configured = any(
        getattr(handler, _HANDLER_MARKER, False) for handler in root_logger.handlers
    )
    if already_configured:
        return

    os.makedirs("logs", exist_ok=True)
    formatter = logging.Formatter(_LOG_FORMAT)

    file_handler = logging.FileHandler("logs/app.log", encoding="utf-8")
    stream_handler = logging.StreamHandler()
    for handler in (file_handler, stream_handler):
        handler.setFormatter(formatter)
        setattr(handler, _HANDLER_MARKER, True)
        root_logger.addHandler(handler)


configure_logging()


async def main() -> None:
    settings = get_settings()
    intake = build_slowlog_intake(settings)
    consumer = build_kafka_consumer(intake, settings)

    try:
        await consumer.run()
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
