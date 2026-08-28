import asyncio
import logging
import os

from cluster_doctor.infrastructure.config.dependencies import (
    build_trigger_service,
    build_kafka_consumer,
    close_clickhouse_client,
)
from cluster_doctor.infrastructure.config.settings import get_settings

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

    service = build_trigger_service(settings)
    consumer = build_kafka_consumer(service, settings)

    try:
        await consumer.run()
    finally:
        close_clickhouse_client()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
