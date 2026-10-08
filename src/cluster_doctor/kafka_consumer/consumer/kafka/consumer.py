"""Kafka consumer 어댑터.

Kafka 메시지를 ProblemLogSignal로 변환해 ProblemLogProcessor에 전달한다.
메시지 파싱에 실패해도 consumer를 죽이지 않고 경고만 남긴다.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener, TopicPartition
from aiokafka.errors import KafkaError

from cluster_doctor.exceptions import KafkaUnavailableError
from cluster_doctor.kafka_consumer.trigger_settling.service.inflow import ProblemLogSignal
from cluster_doctor.kafka_consumer.trigger_settling.service.problem_log_processor import ProblemLogProcessor

_logger = logging.getLogger(__name__)
_HEALTH_CHECK_INTERVAL_SECONDS = 10.0
_HEALTH_CHECK_TIMEOUT_SECONDS = 10.0
_CONSUMER_STOP_TIMEOUT_SECONDS = 10.0


class _AssignmentState(ConsumerRebalanceListener):
    """Distinguish a completed empty assignment from an unfinished rebalance."""

    def __init__(self) -> None:
        self.rebalancing = True
        self.revision = 0

    def on_partitions_revoked(self, revoked) -> None:
        self.rebalancing = True
        self.revision += 1

    def on_partitions_assigned(self, assigned) -> None:
        self.rebalancing = False
        self.revision += 1


class KafkaConsumerAdapter:
    def __init__(
        self,
        problem_log_processor: ProblemLogProcessor,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        failure_timeout_seconds: float = 300.0,
    ) -> None:
        if failure_timeout_seconds <= 0:
            raise ValueError("failure_timeout_seconds must be positive")
        self._failure_timeout_seconds = failure_timeout_seconds
        self._problem_log_processor = problem_log_processor
        self._topic = topic
        self._assignment = _AssignmentState()
        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )
        self._consumer.subscribe(topics=[topic], listener=self._assignment)

    async def run(self) -> None:
        tasks: list[asyncio.Task[None]] = []
        try:
            try:
                await asyncio.wait_for(
                    self._consumer.start(), timeout=self._failure_timeout_seconds
                )
            except (KafkaError, TimeoutError) as exc:
                raise KafkaUnavailableError(
                    f"Kafka consumer could not start ({type(exc).__name__})"
                ) from None
            _logger.info("Kafka consumer started")
            tasks = [
                asyncio.create_task(self._consume_messages()),
                asyncio.create_task(self._watch_connection()),
            ]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            try:
                await asyncio.wait_for(
                    self._consumer.stop(), timeout=_CONSUMER_STOP_TIMEOUT_SECONDS
                )
            except (KafkaError, TimeoutError) as exc:
                _logger.warning(
                    "Kafka consumer cleanup incomplete (%s)", type(exc).__name__
                )
            _logger.info("Kafka consumer loop stopped")

    async def _watch_connection(self) -> None:
        loop = asyncio.get_running_loop()
        failed_since: float | None = None
        while True:
            started = loop.time()
            remaining = self._failure_timeout_seconds
            if failed_since is not None:
                remaining -= started - failed_since
            if remaining <= 0:
                raise KafkaUnavailableError(
                    f"Kafka connection failed continuously for {self._failure_timeout_seconds:g}s"
                )
            try:
                await asyncio.wait_for(
                    self._probe_connection(),
                    timeout=min(_HEALTH_CHECK_TIMEOUT_SECONDS, remaining),
                )
            except (KafkaError, TimeoutError) as exc:
                if failed_since is None:
                    failed_since = started
                    _logger.warning(
                        "Kafka connection check failed (%s); exit after %gs of continuous failure",
                        type(exc).__name__,
                        self._failure_timeout_seconds,
                    )
            else:
                if failed_since is not None:
                    _logger.info("Kafka connection recovered; failure deadline reset")
                failed_since = None
            delay = _HEALTH_CHECK_INTERVAL_SECONDS
            if failed_since is not None:
                remaining = self._failure_timeout_seconds - (loop.time() - failed_since)
                delay = min(delay, max(0, remaining))
            await asyncio.sleep(delay)

    async def _probe_connection(self) -> None:
        revision = self._assignment.revision
        partitions = self._consumer.assignment()
        # 브로커에 새로 요청하지만 consumer 자체의 metadata 캐시는 갱신하지 않는다.
        # 오래된 leader는 end_offsets()가 따로 처리한다.
        if self._topic not in await self._consumer.topics():
            raise KafkaError("Subscribed topic is absent from broker metadata")

        if partitions and not self._assignment.rebalancing:
            offsets = await self._consumer.end_offsets(partitions)
            if any(tp not in offsets or offsets[tp] < 0 for tp in partitions):
                raise KafkaError("Incomplete offsets from assigned partition leaders")
            coordinator_partition = min(partitions)
        else:
            # 파티션을 배정받지 못한 대기 consumer는 다른 consumer 소유 파티션의 leader를 조회하지 않는다.
            known = self._consumer.partitions_for_topic(self._topic)
            if not known:
                raise KafkaError("Subscribed topic has no known partitions")
            coordinator_partition = TopicPartition(self._topic, min(known))

        # 캐시를 거치지 않는 OffsetFetch라 group coordinator까지 함께 검증된다.
        # None은 아직 커밋된 오프셋이 없다는 뜻이며 정상 응답이다.
        await self._consumer.committed(coordinator_partition)
        if (
            self._assignment.rebalancing
            or self._assignment.revision != revision
            or self._consumer.assignment() != partitions
        ):
            raise KafkaError("Partition assignment changed or rebalance is incomplete")

    async def _consume_messages(self) -> None:
        async for msg in self._consumer:
            try:
                data = json.loads(msg.value.decode("utf-8"))
                if isinstance(data, str):
                    data = json.loads(data)
            except Exception as exc:
                _logger.warning(
                    "partition=%d offset=%d JSON 디코딩 실패: %s",
                    msg.partition, msg.offset, exc,
                )
                data = {}

            try:
                problem_log_signal = _parse_message(data)
            except Exception as exc:
                _logger.warning(
                    "partition=%d offset=%d 파싱 실패, 수신 시각으로 폴백: %s",
                    msg.partition, msg.offset, exc,
                )
                problem_log_signal = ProblemLogSignal(timestamp=datetime.now(timezone.utc))
            await self._problem_log_processor.submit(problem_log_signal)


def _parse_message(data: dict) -> ProblemLogSignal:
    """Kafka 메시지에서 발생 시각만 뽑는다.

    이 항목은 ``ProblemLogProcessor``가 유입 정착 범위를 계산하는 데만 쓴다. 상세
    분석은 ClickHouse를 조회해서 하므로 여기서 나머지 필드를 채울 이유가 없다.

    돌려주는 시각은 반드시 timezone-aware다. naive가 하나라도 섞이면
    유입 정착의 시각 정렬이 TypeError로 터져 analysis가 통째로 죽는 것을
    막기 위해서다.
    ``astimezone``도 naive 값에는 호스트 로컬 시간대를 가정해 분석 구간을
    9시간 어긋나게 한다.
    """
    src = data.get("_source", data)

    raw_ts = src.get("@timestamp") or src.get("timestamp") or src.get("time")
    if isinstance(raw_ts, str):
        timestamp = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
    elif isinstance(raw_ts, (int, float)):
        timestamp = datetime.fromtimestamp(raw_ts / 1000, tz=timezone.utc)
    else:
        timestamp = datetime.now(timezone.utc)

    if timestamp.utcoffset() is None:
        # 오프셋이 없는 문자열은 UTC로 읽는다. 파이프라인이 보내는 값에는
        # 오프셋이 붙어 있으므로("...+09:00") 이 경로는 형식이 어긋났을 때만
        # 탄다. 어긋난 값을 KST로 가정하면 조용히 9시간 밀리므로, 덜 틀리는
        # 쪽이 아니라 명시적인 쪽을 고른다.
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    return ProblemLogSignal(timestamp=timestamp)
