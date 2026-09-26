import asyncio
from types import SimpleNamespace

from cluster_doctor.domain.incident.models import SlowlogTrigger


class RecordingIntake:
    def __init__(self) -> None:
        self.triggers = []

    async def handle(self, trigger) -> None:
        self.triggers.append(trigger)


async def test_consumer_deserializes_message_and_delegates_to_slowlog_intake(monkeypatch):
    from cluster_doctor.adapters.inbound.kafka.consumer import KafkaConsumerAdapter
    import cluster_doctor.adapters.inbound.kafka.consumer as consumer_module

    intake = RecordingIntake()
    message = SimpleNamespace(
        value=b'{"_source":{"@timestamp":"2026-09-26T10:00:00Z"}}',
        partition=1,
        offset=2,
    )

    class FakeConsumer:
        async def start(self):
            pass

        async def stop(self):
            pass

        def __aiter__(self):
            async def messages():
                yield message
            return messages()

    monkeypatch.setattr(consumer_module, "AIOKafkaConsumer", lambda *a, **kw: FakeConsumer())
    adapter = KafkaConsumerAdapter(intake, "broker:9092", "slowlog", "group")

    task = asyncio.create_task(adapter.run())
    for _ in range(20):
        if intake.triggers:
            break
        await asyncio.sleep(0)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert len(intake.triggers) == 1
    assert isinstance(intake.triggers[0], SlowlogTrigger)
    assert intake.triggers[0].timestamp.isoformat() == "2026-09-26T10:00:00+00:00"
