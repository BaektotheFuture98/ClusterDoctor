"""Driving port for accepted slowlog triggers."""

from __future__ import annotations

from typing import Protocol

from cluster_doctor.kafka_consumer.trigger_settling.service.inflow import SlowlogTrigger


class SlowlogHandler(Protocol):
    """Accept a parsed slowlog trigger from an inbound adapter."""

    async def handle(self, trigger: SlowlogTrigger) -> None: ...
