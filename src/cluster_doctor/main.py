import asyncio
import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

from cluster_doctor.bootstrap.configuration.settings import LoggingSettings, get_settings
from cluster_doctor.bootstrap.dependency.wiring import (
    build_kafka_consumer,
    build_runtime_resources,
    build_problem_log_processor,
)
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

    logging_settings = LoggingSettings()
    log_dir = Path(logging_settings.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(_LOG_FORMAT)
    incident_id_filter = IncidentIdLogFilter()

    file_handler = RotatingFileHandler(
        log_dir / "app.log",
        maxBytes=logging_settings.log_max_bytes,
        backupCount=logging_settings.log_backup_count,
        encoding="utf-8",
    )
    stream_handler = logging.StreamHandler()
    for handler in (file_handler, stream_handler):
        handler.setFormatter(formatter)
        handler.addFilter(incident_id_filter)
        setattr(handler, _HANDLER_MARKER, True)
        root_logger.addHandler(handler)


configure_logging()


async def main() -> None:
    settings = get_settings()
    with build_runtime_resources(settings) as runtime_resources:
        problem_log_processor = build_problem_log_processor(settings, runtime_resources)
        try:
            kafka_consumer = build_kafka_consumer(problem_log_processor, settings)
            try:
                await kafka_consumer.run()
            except KafkaUnavailableError as exc:
                logging.getLogger(__name__).critical("%s; terminating process with exit code 1", exc)
                logging.shutdown()
                # consumer 정리는 이미 시도했다. 정상 종료는 분석 스레드를 기다리므로
                # 이 치명적 장애 정책을 지킬 수 없다. 대기·진행 중 분석을 버리고
                # 프로세스 전체를 즉시 끝낸다.
                os._exit(1)
        finally:
            await problem_log_processor.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Received keyboard interrupt, shutting down...")
        pass
