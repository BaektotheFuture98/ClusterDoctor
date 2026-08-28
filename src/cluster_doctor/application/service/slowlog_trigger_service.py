"""Kafka slowlog 수신 시 agent를 트리거한다.

micro_batch_seconds 동안 slowlog를 모은 뒤 agent를 한 번 실행한다.
  - 첫 slowlog 수신 → 타이머 시작, pending 큐에 적재
  - 타이머 만료 → agent 실행 (pending 큐의 로그는 agent가 check_new_slowlogs()로 꺼냄)
  - agent 실행 중 도착한 slowlog → pending 큐에 적재 (agent가 직접 확인)
  - agent가 성공으로 끝났고 큐에 잔여 항목이 남아 있으면 재트리거.
    단 micro_batch_seconds만큼 쉰 뒤에 걸고, 연속 3회를 넘기지 않는다.
    실패한 실행은 재트리거하지 않는다 (다음 slowlog 도착 시 자연히 재개된다).
"""

import asyncio
import logging
import queue as stdlib_queue
from collections.abc import Coroutine
from datetime import datetime, timezone

from cluster_doctor.application.port.outbound.llm_analyzer import (
    LlmAnalyzer,
    LlmApiError,
    LlmResponseError,
)
from cluster_doctor.application.port.outbound.notifier import Notifier
from cluster_doctor.domain.model.log_entry import LogEntry

_logger = logging.getLogger(__name__)

# agent가 큐를 비우지 않은 채 계속 성공하면 재트리거가 끝나지 않는다
# (큐를 비우는 것은 agent의 check_new_slowlogs 뿐이다). 상한을 둬서
# 프롬프트를 어긴 실행이 무한 루프가 되지 않게 한다.
_MAX_CONSECUTIVE_RETRIGGERS = 3


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
        self._agent_task: asyncio.Task | None = None
        self._consecutive_retriggers = 0
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
        self._consecutive_retriggers = 0
        self._spawn_agent(self._first_log_time, self._first_kafka_receive_time)

    def _spawn(self, coro: Coroutine) -> None:
        """태스크를 띄우고 핸들을 보관한다.

        asyncio는 실행 중인 태스크에 약한 참조만 유지한다. create_task의
        반환값을 버리면 실행 도중 GC되어 진단이 아무 흔적 없이 사라질 수 있다.
        """
        self._agent_task = asyncio.create_task(coro)

    def _spawn_agent(self, log_time: datetime, kafka_receive_time: datetime) -> None:
        """agent를 즉시 실행한다. 첫 실행 경로 — _wait_and_trigger가 이미 기다렸다."""
        self._spawn(self._run_agent(log_time, kafka_receive_time))

    def _spawn_delayed_agent(
        self, log_time: datetime, kafka_receive_time: datetime
    ) -> None:
        """재실행을 건다. _maybe_retrigger는 동기 함수라 직접 await할 수 없다."""
        self._spawn(self._delayed_agent(log_time, kafka_receive_time))

    async def _delayed_agent(self, log_time: datetime, kafka_receive_time: datetime) -> None:
        """재실행 전에 배치 창만큼 쉰다.

        첫 실행은 _wait_and_trigger가 이미 기다렸으므로 이 경로를 타지 않는다.
        재실행에 지연이 없으면 실패한 실행이 지연 0으로 연달아 돌아, 상한에
        걸릴 때까지 할당량을 그대로 태운다.
        """
        await asyncio.sleep(self._micro_batch_seconds)
        await self._run_agent(log_time, kafka_receive_time)

    async def _run_agent(self, log_time: datetime, kafka_receive_time: datetime) -> None:
        _logger.info(
            "agent 시작 (log_time=%s, kafka_receive_time=%s)",
            log_time.strftime("%Y-%m-%d %H:%M:%S"),
            kafka_receive_time.strftime("%Y-%m-%d %H:%M:%S"),
        )
        succeeded = False
        try:
            report = await asyncio.to_thread(
                self._llm_analyzer.analyze, log_time, kafka_receive_time
            )
            _logger.info("agent 완료 — 리포트 전송")
            await self._notifier.notify(report)
            succeeded = True
        except (LlmApiError, LlmResponseError) as exc:
            # 두 예외는 상속 관계가 없는 형제다(둘 다 RuntimeError 직속).
            # LlmApiError만 잡으면 빈 응답(LlmResponseError)이 아래
            # generic 핸들러로 빠져 '예상치 못한 오류'로 잘못 분류된다.
            _logger.error("LLM 분석 실패: %s", exc)
        except Exception:
            # exc_info를 남긴다. 이 갈래는 원인을 모르는 실패이므로
            # 스택 없이는 진단할 수 없다.
            _logger.exception("agent 실행 중 예상치 못한 오류")
        finally:
            self._running = False
            self._agent_task = None
            self._maybe_retrigger(succeeded)

    def _maybe_retrigger(self, succeeded: bool) -> None:
        """다음 실행을 이어서 걸지 결정한다.

        실패한 실행은 절대 이어 걸지 않는다. _run_agent은 큐를 비우지 않으므로
        (비우는 것은 agent의 check_new_slowlogs 뿐이다) 실패하면 큐가 그대로
        남고, 바로 다시 걸면 같은 실패를 백오프 없이 무한 반복하며 API
        할당량을 태운다. 다음 slowlog가 도착하면 on_slowlog가 새 타이머를
        걸어 자연히 재개되므로 잃는 것은 없다.
        """
        if not succeeded:
            self._consecutive_retriggers = 0
            return

        if self._pending.empty():
            self._consecutive_retriggers = 0
            return

        if self._consecutive_retriggers >= _MAX_CONSECUTIVE_RETRIGGERS:
            _logger.warning(
                "연속 재트리거 %d회에 도달해 중단한다. agent가 큐를 비우지 "
                "않고 있다(check_new_slowlogs 미호출 가능성). 다음 slowlog "
                "도착 시 재개된다.",
                self._consecutive_retriggers,
            )
            self._consecutive_retriggers = 0
            return

        self._consecutive_retriggers += 1
        self._running = True
        now = datetime.now(timezone.utc)
        self._spawn_delayed_agent(now, now)
