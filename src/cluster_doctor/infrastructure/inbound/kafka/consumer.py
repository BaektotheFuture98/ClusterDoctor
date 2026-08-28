"""Kafka consumer 어댑터.

Kafka 메시지를 LogEntry로 변환해 SlowlogTriggerService에 전달한다.
메시지 파싱에 실패해도 consumer를 죽이지 않고 경고만 남긴다.
"""

import json
import logging
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer

from cluster_doctor.application.service.slowlog_trigger_service import SlowlogTriggerService
from cluster_doctor.domain.model.log_entry import SlowlogEntry

_logger = logging.getLogger(__name__)


class KafkaConsumerAdapter:
    def __init__(
        self,
        service: SlowlogTriggerService,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
    ) -> None:
        self._service = service
        self._consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )

    async def run(self) -> None:
        try:
            await self._consumer.start()
            _logger.info("Kafka consumer started")
            async for msg in self._consumer:
                try:
                    data = json.loads(msg.value.decode("utf-8"))
                    if isinstance(data, str):
                        data = json.loads(data)
                except Exception as exc:
                    _logger.warning(
                        "partition=%d offset=%d JSON 디코딩 실패: %s",
                        msg.partition,
                        msg.offset,
                        exc,
                    )
                    data = {}

                try:
                    log_entry = _parse_message(data)
                except Exception as exc:
                    _logger.warning(
                        "partition=%d offset=%d 파싱 실패, 수신 시각으로 폴백: %s",
                        msg.partition,
                        msg.offset,
                        exc,
                    )
                    log_entry = SlowlogEntry(timestamp=datetime.now(timezone.utc))

                await self._service.on_slowlog(log_entry)
        finally:
            await self._consumer.stop()
            _logger.info("Kafka consumer stopped")


def _parse_message(data: dict) -> SlowlogEntry:
    """Kafka 메시지에서 발생 시각만 뽑는다.

    이 항목은 트리거 큐에 들어가 "언제 얼마나 들어왔나"를 세는 데만 쓰인다
    (``check_new_slowlogs``). 내용 분석은 ClickHouse를 조회해서 하므로 여기서
    나머지 필드까지 채울 이유가 없다.
    """
    src = data.get("_source", data)

    raw_ts = src.get("@timestamp") or src.get("timestamp") or src.get("time")
    if isinstance(raw_ts, str):
        timestamp = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
    elif isinstance(raw_ts, (int, float)):
        timestamp = datetime.fromtimestamp(raw_ts / 1000, tz=timezone.utc)
    else:
        timestamp = datetime.now(timezone.utc)

    return SlowlogEntry(timestamp=timestamp)
