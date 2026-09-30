"""Incident 상관관계를 로그에 자동으로 싣는다.

``bind_incident_id``로 감싼 구간에서 나오는 모든 로그 레코드는, 어느
로거·어느 스레드에서 났든 ``incident_id``를 갖는다(``asyncio.to_thread``가
contextvars를 복사해서 스레드로 넘기므로). 각 로그 문구에 ``incident.incident_id``를
매번 손으로 실어 나르지 않아도 되는 이유다.

바깥(예: Kafka 정착 대기처럼 Incident가 아직 없는 구간)에서는 ``IncidentIdLogFilter``가
기본값 ``"-"``를 채운다 — 포맷 문자열이 ``%(incident_id)s``를 항상 기대하므로,
필터가 없으면 그 구간의 로그가 포맷 단계에서 죽는다.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

_incident_id: ContextVar[str] = ContextVar("incident_id", default="-")


@contextmanager
def bind_incident_id(incident_id: str) -> Iterator[None]:
    token = _incident_id.set(incident_id)
    try:
        yield
    finally:
        _incident_id.reset(token)


class IncidentIdLogFilter(logging.Filter):
    """모든 ``LogRecord``에 현재 바인딩된 ``incident_id``를 붙인다."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.incident_id = _incident_id.get()
        return True
