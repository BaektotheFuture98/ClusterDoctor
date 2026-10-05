import asyncio
from collections import deque
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from aiokafka.errors import KafkaConnectionError, KafkaError, NotLeaderForPartitionError
from aiokafka import TopicPartition
from aiokafka.consumer.fetcher import Fetcher
from aiokafka.protocol.metadata import MetadataResponse_v0
from aiokafka.protocol.offset import OffsetResponse_v1

from cluster_doctor.exceptions import KafkaUnavailableError
from cluster_doctor.kafka_consumer.consumer.kafka import consumer as consumer_module


class FakeConsumer:
    def __init__(
        self, *, results=(), hang_probe=False, hang_stop=False,
        fail_start=False, hang_start=False, messages=(),
        leader_results=(), coordinator_results=(), assigned=None,
        topic_partitions=None, topics=None, delays=None,
    ):
        self.results = deque(results)
        self.hang_probe = hang_probe
        self.hang_stop = hang_stop
        self.probes = 0
        self.stopped = False
        self.fail_start = fail_start
        self.hang_start = hang_start
        self.messages = deque(messages)
        self.leader_results = deque(leader_results)
        self.coordinator_results = deque(coordinator_results)
        self.assigned = {TopicPartition("slowlog", 0)} if assigned is None else set(assigned)
        self.topic_partitions = {0} if topic_partitions is None else topic_partitions
        self.available_topics = {"slowlog"} if topics is None else topics
        self.delays = delays or {}
        self.listener = None

    def subscribe(self, topics, listener=None):
        self.listener = listener

    def rebalance(self, assigned=None):
        if self.listener:
            self.listener.on_partitions_revoked(self.assigned)
        self.assigned = set() if assigned is None else set(assigned)
        if assigned is not None and self.listener:
            self.listener.on_partitions_assigned(self.assigned)

    def assignment(self):
        return self.assigned.copy()

    def partitions_for_topic(self, topic):
        return self.topic_partitions

    async def end_offsets(self, partitions):
        await asyncio.sleep(self.delays.get("leaders", 0))
        result = self.leader_results.popleft() if self.leader_results else True
        if result is False:
            raise KafkaConnectionError("leader unavailable")
        if isinstance(result, dict):
            return result
        return {tp: 0 for tp in partitions}

    async def committed(self, partition):
        await asyncio.sleep(self.delays.get("coordinator", 0))
        result = self.coordinator_results.popleft() if self.coordinator_results else True
        if result is False:
            raise KafkaConnectionError("coordinator unavailable")
        return None

    async def start(self):
        if self.fail_start:
            raise KafkaConnectionError("broker offline")
        if self.hang_start:
            await asyncio.Event().wait()
        if self.listener:
            self.listener.on_partitions_assigned(self.assigned)

    async def stop(self):
        self.stopped = True
        if self.hang_stop:
            await asyncio.Event().wait()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.messages:
            return self.messages.popleft()
        await asyncio.Event().wait()

    async def topics(self):
        self.probes += 1
        await asyncio.sleep(self.delays.get("metadata", 0))
        if self.hang_probe:
            await asyncio.Event().wait()
        available = self.results.popleft() if self.results else True
        if not available:
            raise KafkaConnectionError("broker offline")
        return self.available_topics


def adapter(monkeypatch, fake):
    monkeypatch.setattr(consumer_module, "AIOKafkaConsumer", lambda *args, **kwargs: fake)
    monkeypatch.setattr(consumer_module, "_HEALTH_CHECK_INTERVAL_SECONDS", 0.01, raising=False)
    monkeypatch.setattr(consumer_module, "_HEALTH_CHECK_TIMEOUT_SECONDS", 0.02, raising=False)
    monkeypatch.setattr(consumer_module, "_CONSUMER_STOP_TIMEOUT_SECONDS", 0.02, raising=False)
    result = consumer_module.KafkaConsumerAdapter(
        problem_log_processor=None, bootstrap_servers="localhost:9092", topic="slowlog", group_id="test",
        failure_timeout_seconds=0.05,
    )
    return result


@pytest.mark.parametrize("hang_probe,hang_stop", [(False, False), (True, False), (False, True)])
async def test_continuous_connection_failure_stops_consumer(monkeypatch, hang_probe, hang_stop):
    fake = FakeConsumer(results=[False] * 100, hang_probe=hang_probe, hang_stop=hang_stop)
    task = asyncio.create_task(adapter(monkeypatch, fake).run())
    try:
        done, _ = await asyncio.wait({task}, timeout=0.3)
        assert task in done, "Kafka outage must terminate the consumer loop"
        assert isinstance(task.exception(), KafkaUnavailableError)
        assert fake.stopped
    finally:
        task.cancel()
        try:
            await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), timeout=0.1)
        except TimeoutError:
            pass


@pytest.mark.parametrize("results", [[], [False, False, True, False, False, True]])
async def test_idle_topic_and_recovered_connection_keep_consumer_running(monkeypatch, results):
    fake = FakeConsumer(results=results)
    task = asyncio.create_task(adapter(monkeypatch, fake).run())
    try:
        await asyncio.sleep(0.15)
        assert not task.done()
        assert fake.probes >= 6
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert fake.stopped


@pytest.mark.parametrize("kwargs", [
    {"leader_results": [False] * 100},
    {"coordinator_results": [False] * 100},
    {"leader_results": [{}] * 100},
    {"leader_results": [{TopicPartition("slowlog", 0): -1}] * 100},
    {"topics": set()},
    {"assigned": set(), "topic_partitions": set()},
    {"assigned": set(), "coordinator_results": [False] * 100},
    {"delays": {"leaders": 10}},
    {"delays": {"coordinator": 10}},
    {
        "assigned": {TopicPartition("slowlog", 0), TopicPartition("slowlog", 1)},
        "leader_results": [{TopicPartition("slowlog", 0): 0}] * 100,
    },
])
async def test_metadata_success_does_not_hide_subscription_failure(monkeypatch, kwargs):
    fake = FakeConsumer(**kwargs)
    task = asyncio.create_task(adapter(monkeypatch, fake).run())
    try:
        done, _ = await asyncio.wait({task}, timeout=0.3)
        assert task in done, "metadata success must not hide subscription failures"
        assert isinstance(task.exception(), KafkaUnavailableError)
        assert fake.stopped
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("stage", ["leader_results", "coordinator_results"])
async def test_subscription_recovery_resets_deadline(monkeypatch, stage):
    fake = FakeConsumer(**{stage: [False, False, True] * 8})
    task = asyncio.create_task(adapter(monkeypatch, fake).run())
    try:
        await asyncio.sleep(0.16)
        assert not task.done()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_standby_does_not_probe_partitions_owned_by_other_consumers(monkeypatch):
    fake = FakeConsumer(assigned=set(), leader_results=[False] * 100)
    task = asyncio.create_task(adapter(monkeypatch, fake).run())
    try:
        await asyncio.sleep(0.15)
        assert not task.done()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_rebalancing_does_not_erase_existing_failure_deadline(monkeypatch):
    fake = FakeConsumer(leader_results=[False] * 100)
    task = asyncio.create_task(adapter(monkeypatch, fake).run())
    try:
        await asyncio.sleep(0.025)
        fake.rebalance()
        done, _ = await asyncio.wait({task}, timeout=0.2)
        assert task in done, "losing assignment during rebalance must not reset failure time"
        assert isinstance(task.exception(), KafkaUnavailableError)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_combined_probe_has_one_timeout(monkeypatch):
    fake = FakeConsumer(delays={"metadata": 0.008, "leaders": 0.008, "coordinator": 0.008})
    task = asyncio.create_task(adapter(monkeypatch, fake).run())
    try:
        done, _ = await asyncio.wait({task}, timeout=0.3)
        assert task in done, "the combined probe must not get a fresh timeout per stage"
        assert isinstance(task.exception(), KafkaUnavailableError)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_assignment_change_during_probe_is_not_a_recovery(monkeypatch):
    fake = FakeConsumer(delays={"coordinator": 0.03})
    consumer = adapter(monkeypatch, fake)
    await fake.start()
    probe = asyncio.create_task(consumer._probe_connection())
    try:
        await asyncio.sleep(0.01)
        # Even identical partition sets belong to a different group generation.
        fake.rebalance({TopicPartition("slowlog", 0)})
        with pytest.raises(KafkaError, match="assignment changed"):
            await probe
    finally:
        probe.cancel()
        await asyncio.gather(probe, return_exceptions=True)


async def test_completed_rebalance_to_standby_can_recover(monkeypatch):
    fake = FakeConsumer(leader_results=[False] * 100)
    task = asyncio.create_task(adapter(monkeypatch, fake).run())
    try:
        await asyncio.sleep(0.025)
        fake.rebalance(set())
        await asyncio.sleep(0.12)
        assert not task.done(), "a confirmed standby no longer needs the old partition leader"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_short_rebalance_does_not_terminate_healthy_consumer(monkeypatch):
    fake = FakeConsumer()
    task = asyncio.create_task(adapter(monkeypatch, fake).run())
    try:
        await asyncio.sleep(0.015)
        fake.rebalance()
        await asyncio.sleep(0.015)
        fake.rebalance({TopicPartition("slowlog", 0)})
        await asyncio.sleep(0.12)
        assert not task.done()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@asynccontextmanager
async def library_consumer(monkeypatch, *, leader, cached_leader=None, unreachable=False):
    """Keep aiokafka metadata/ListOffsets processing real; replace broker I/O only."""
    async def idle_fetch(self):
        await asyncio.Event().wait()

    monkeypatch.setattr(Fetcher, "_fetch_requests_routine", idle_fetch)
    monkeypatch.setattr(consumer_module, "_HEALTH_CHECK_INTERVAL_SECONDS", 0.005)
    monkeypatch.setattr(consumer_module, "_HEALTH_CHECK_TIMEOUT_SECONDS", 0.015)
    consumer = consumer_module.KafkaConsumerAdapter(
        problem_log_processor=None, bootstrap_servers="unused:9092", topic="slowlog",
        group_id="test", failure_timeout_seconds=0.04,
    )
    raw = consumer._consumer
    client = raw._client
    partition = TopicPartition("slowlog", 0)

    def metadata(current_leader):
        return MetadataResponse_v0(
            brokers=[(1, "metadata-broker", 9092), (2, "old-leader", 9092), (3, "new-leader", 9092)],
            topics=[(0, "slowlog", [(5 if current_leader == -1 else 0, 0, current_leader, [2, 3], [])])],
        )

    response = metadata(leader)
    client.cluster.update_metadata(metadata(leader if cached_leader is None else cached_leader))
    # subscribe() queued a metadata refresh before the I/O seam was installed.
    # Model its completed bootstrap so ListOffsets can reach the broker seam.
    if client._md_update_fut is not None:
        client._md_update_fut.set_result(True)
        client._md_update_fut = None
    raw._subscription.assign_from_subscribed([partition])
    consumer._assignment.on_partitions_assigned({partition})
    raw._fetcher = Fetcher(client, raw._subscription, retry_backoff_ms=1)

    class MetadataConnection:
        async def send(self, request):
            return response

    async def get_connection(*args, **kwargs):
        return MetadataConnection()

    async def send_offsets(node_id, request):
        if unreachable:
            raise KafkaConnectionError("assigned leader offline")
        if node_id != leader:
            raise NotLeaderForPartitionError()
        return OffsetResponse_v1(topics=[("slowlog", [(0, 0, -1, 0)])])

    def refresh():
        client.cluster.update_metadata(response)
        future = asyncio.get_running_loop().create_future()
        future.set_result(True)
        return future

    async def committed_offsets(partitions):
        return {}

    async def close_coordinator():
        pass

    monkeypatch.setattr(client, "_get_conn", get_connection)
    monkeypatch.setattr(client, "send", send_offsets)
    monkeypatch.setattr(client, "force_metadata_update", refresh)
    raw._coordinator = SimpleNamespace(fetch_committed_offsets=committed_offsets, close=close_coordinator)
    try:
        yield consumer
    finally:
        await raw.stop()


@pytest.mark.parametrize("leader,unreachable", [(-1, False), (2, True)])
async def test_real_aiokafka_metadata_does_not_hide_leader_outage(monkeypatch, leader, unreachable):
    async with library_consumer(monkeypatch, leader=leader, unreachable=unreachable) as consumer:
        assert await consumer._consumer.topics() == {"slowlog"}
        with pytest.raises(KafkaUnavailableError):
            await asyncio.wait_for(consumer._watch_connection(), timeout=0.3)


async def test_real_aiokafka_refreshes_stale_leader_without_consuming(monkeypatch):
    async with library_consumer(monkeypatch, leader=3, cached_leader=2) as consumer:
        raw = consumer._consumer
        partition = TopicPartition("slowlog", 0)
        assert await raw.topics() == {"slowlog"}
        assert raw._client.cluster.leader_for_partition(partition) == 2
        before = raw._subscription.subscription.assignment.all_consumed_offsets()
        await asyncio.wait_for(consumer._probe_connection(), timeout=0.3)
        assert raw._client.cluster.leader_for_partition(partition) == 3
        assert raw._subscription.subscription.assignment.all_consumed_offsets() == before


@pytest.mark.parametrize("hang_start", [False, True])
async def test_startup_connection_failure_is_bounded(monkeypatch, hang_start):
    fake = FakeConsumer(fail_start=not hang_start, hang_start=hang_start)
    with pytest.raises(KafkaUnavailableError):
        await asyncio.wait_for(adapter(monkeypatch, fake).run(), timeout=0.3)
    assert fake.stopped


async def test_watchdog_preserves_message_delivery(monkeypatch):
    fake = FakeConsumer(messages=[SimpleNamespace(
        value=b'{"_source":{"@timestamp":"2026-10-01T00:00:00Z"}}',
        partition=0, offset=1,
    )])
    received = []
    delivered = asyncio.Event()

    async def submit(problem_log_signal):
        received.append(problem_log_signal.timestamp)
        delivered.set()

    consumer = adapter(monkeypatch, fake)
    consumer._problem_log_processor = SimpleNamespace(submit=submit)
    task = asyncio.create_task(consumer.run())
    try:
        await asyncio.wait_for(delivered.wait(), timeout=0.3)
        assert received == [datetime(2026, 10, 1, tzinfo=UTC)]
        assert not task.done()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
