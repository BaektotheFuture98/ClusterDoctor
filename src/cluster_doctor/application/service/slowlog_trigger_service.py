"""Kafka slowlog 수신 시 agent를 트리거한다.

micro_batch_seconds 동안 slowlog를 모은 뒤 agent를 한 번 실행한다.
  - 첫 slowlog 수신 → 타이머 시작, pending 큐에 적재
  - 타이머 만료 → agent 실행 (pending 큐의 로그는 agent가 check_new_slowlogs()로 꺼냄)
  - agent 실행 중 도착한 slowlog → pending 큐에 적재 (agent가 직접 확인)
  - agent 종료 후 큐에 잔여 항목 → 재트리거
"""

import asyncio
import logging
import queue as stdlib_queue
from datetime import datetime, timezone

from cluster_doctor.application.port.outbound.llm_analyzer import (
    LlmAnalyzer,
    LlmApiError,
    LlmResponseError,
)
from cluster_doctor.application.port.outbound.notifier import Notifier
from cluster_doctor.domain.model.log_entry import LogEntry

_logger = logging.getLogger(__name__)


class SlowlogTriggerService:
    def __init__(
        self,
        llm_analyzer: LlmAnalyzer,
        notifier: Notifier,
        pending: stdlib_queue.Queue,
        micro_batch_seconds: float = 10.0,
    ) -> None:
        self._llm_analyzer = llm_analyzer
        self._notifier = notifier
        self._pending = pending
        self._micro_batch_seconds = micro_batch_seconds
        self._running = False
        self._trigger_task: asyncio.Task | None = None
        self._first_log_time: datetime | None = None
        self._first_kafka_receive_time: datetime | None = None

    async def on_slowlog(self, log_entry: LogEntry) -> None:
        """Kafka consumer가 slowlog를 수신할 때마다 호출한다."""
        self._pending.put(log_entry)

        if self._running:
            return

        if self._trigger_task is None:
            self._first_log_time = log_entry.timestamp
            self._first_kafka_receive_time = datetime.now(timezone.utc)
            self._trigger_task = asyncio.create_task(self._wait_and_trigger())

    async def _wait_and_trigger(self) -> None:
        """micro_batch_seconds 후 agent를 실행한다."""
        await asyncio.sleep(self._micro_batch_seconds)
        self._trigger_task = None
        self._running = True
        asyncio.create_task(self._run_agent(self._first_log_time, self._first_kafka_receive_time))

    async def _run_agent(self, log_time: datetime, kafka_receive_time: datetime) -> None:
        _logger.info(
            "agent 시작 (log_time=%s, kafka_receive_time=%s)",
            log_time.strftime("%Y-%m-%d %H:%M:%S"),
            kafka_receive_time.strftime("%Y-%m-%d %H:%M:%S"),
        )
        try:
            report = await asyncio.to_thread(
                self._llm_analyzer.analyze, log_time, kafka_receive_time
            )
            _logger.info("agent 완료 — 리포트 전송")
            await self._notifier.notify(report)
        except (LlmApiError, LlmResponseError) as exc:
            # 두 예외는 상속 관계가 없는 형제다(둘 다 RuntimeError 직속).
            # LlmApiError만 잡으면 빈 응답(LlmResponseError)이 아래
            # generic 핸들러로 빠져 '예상치 못한 오류'로 잘못 분류된다.
            _logger.error("LLM 분석 실패: %s", exc)
        except Exception as exc:
            _logger.error("agent 실행 중 예상치 못한 오류: %s", exc)
        finally:
            self._running = False
            if not self._pending.empty():
                self._running = True
                now = datetime.now(timezone.utc)
                asyncio.create_task(self._run_agent(now, now))
