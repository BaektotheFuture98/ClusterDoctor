"""트리거 서비스의 재실행 규칙.

_run_agent은 pending 큐를 비우지 않는다. 큐를 비우는 것은 agent가 부르는
check_new_slowlogs 뿐이다. 그래서 analyze가 실패하면 큐가 손대지지 않은 채
남고, finally가 그대로 재실행을 걸면 같은 실패를 무한히 반복하며 API
할당량만 태운다. 429(할당량 소진)에서 실제로 성립하는 조건이다.
"""

import asyncio
import queue as stdlib_queue
import threading
from datetime import datetime, timezone

from cluster_doctor.application.port.outbound.llm_analyzer import LlmApiError
from cluster_doctor.application.service.slowlog_trigger_service import (
    _MAX_CONSECUTIVE_RETRIGGERS,
    SlowlogTriggerService,
)
from cluster_doctor.domain.model.log_entry import SlowlogEntry

TS = datetime(2026, 8, 28, 10, 20, tzinfo=timezone.utc)


class _Analyzer:
    """analyze는 동기 함수다(서비스가 asyncio.to_thread로 감싼다)."""

    def __init__(self, error: Exception | None = None, drains: stdlib_queue.Queue | None = None):
        self.calls = 0
        self._error = error
        self._drains = drains

    def analyze(self, log_time, kafka_receive_time) -> str:
        self.calls += 1
        if self._error is not None:
            raise self._error
        if self._drains is not None:
            # 정상 agent는 check_new_slowlogs로 큐를 비운다. 그 동작을 흉내낸다.
            while not self._drains.empty():
                self._drains.get_nowait()
        return "리포트"


class _Notifier:
    def __init__(self):
        self.reports = []

    async def notify(self, message: str) -> None:
        self.reports.append(message)


def _service(analyzer, pending):
    return SlowlogTriggerService(
        llm_analyzer=analyzer,
        notifier=_Notifier(),
        pending=pending,
        micro_batch_seconds=0.01,
    )


async def _settle(service, timeout: float = 5.0):
    """재트리거 연쇄가 모두 끝날 때까지 기다린다.

    analyze는 asyncio.to_thread로 실제 스레드에서 돈다. asyncio.sleep(0)만
    돌려서는 그 스레드가 끝났다는 보장이 없으므로, 서비스가 유휴 상태로
    돌아온 것을 조건으로 삼는다.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        await asyncio.sleep(0.01)
        if service._agent_task is None and not service._running:
            return
    raise AssertionError("agent 태스크가 제한 시간 안에 끝나지 않았다")


async def test_failed_run_does_not_retrigger():
    pending = stdlib_queue.Queue()
    pending.put(SlowlogEntry(timestamp=TS))
    analyzer = _Analyzer(error=LlmApiError("호출 실패 (status=429)"))
    service = _service(analyzer, pending)

    await service._run_agent(TS, TS)
    await _settle(service)

    assert analyzer.calls == 1, "실패한 실행을 다시 걸었다"
    assert not pending.empty(), "실패한 실행은 큐를 비우지 않는다"


async def test_unexpected_failure_also_does_not_retrigger():
    # 429는 LlmApiError로 오지만, ES 접속 불가 등은 generic 핸들러로 온다.
    pending = stdlib_queue.Queue()
    pending.put(SlowlogEntry(timestamp=TS))
    analyzer = _Analyzer(error=RuntimeError("ES 접속 불가"))
    service = _service(analyzer, pending)

    await service._run_agent(TS, TS)
    await _settle(service)

    assert analyzer.calls == 1


async def test_successful_run_retriggers_when_the_queue_still_has_items():
    # agent가 큐를 비우지 않고 끝났고 잔여가 있으면 이어서 한 번 더 돈다.
    pending = stdlib_queue.Queue()
    pending.put(SlowlogEntry(timestamp=TS))
    analyzer = _Analyzer()
    service = _service(analyzer, pending)

    await service._run_agent(TS, TS)
    await _settle(service)

    assert analyzer.calls > 1


async def test_consecutive_retriggers_are_capped():
    # agent가 끝내 큐를 비우지 않으면 성공 경로에서도 무한히 돈다.
    pending = stdlib_queue.Queue()
    pending.put(SlowlogEntry(timestamp=TS))
    analyzer = _Analyzer()
    service = _service(analyzer, pending)

    await service._run_agent(TS, TS)
    await _settle(service)

    assert analyzer.calls == 1 + _MAX_CONSECUTIVE_RETRIGGERS


async def test_no_retrigger_when_the_agent_drained_the_queue():
    pending = stdlib_queue.Queue()
    pending.put(SlowlogEntry(timestamp=TS))
    analyzer = _Analyzer(drains=pending)
    service = _service(analyzer, pending)

    await service._run_agent(TS, TS)
    await _settle(service)

    assert analyzer.calls == 1
    assert pending.empty()


async def test_agent_task_handle_is_kept_while_running():
    # asyncio는 실행 중인 태스크에 약한 참조만 유지한다. 핸들을 버리면
    # 실행 도중 GC되어 진단이 조용히 사라질 수 있다.
    #
    # analyze는 워커 스레드에서 돌므로 신호는 threading.Event로 주고받는다.
    # asyncio.Event는 스레드 안전하지 않다.
    pending = stdlib_queue.Queue()
    started = threading.Event()
    release = threading.Event()

    class _Blocking:
        def analyze(self, log_time, kafka_receive_time) -> str:
            started.set()
            release.wait(timeout=5)
            return "리포트"

    service = _service(_Blocking(), pending)
    service._running = True
    service._spawn_agent(TS, TS)

    for _ in range(500):
        if started.is_set():
            break
        await asyncio.sleep(0.01)
    assert started.is_set(), "agent가 시작되지 않았다"

    assert service._agent_task is not None, "실행 중인 태스크 핸들이 보관되지 않았다"

    release.set()
    await service._agent_task
