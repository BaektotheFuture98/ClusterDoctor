"""monitoring-wiki MCP 서버 클라이언트.

메인 event loop와 독립된 스레드에서 asyncio.run()을 쓴다.
report_update()는 동기 함수이고, ClusterGuard 프로세스는 이미 asyncio
loop 위에서 돌기 때문에 같은 스레드에서 새 loop를 열 수 없다.
"""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

_logger = logging.getLogger(__name__)

# 연결·initialize·tool 호출을 모두 합친 상한이다.
_TIMEOUT_SECONDS = 30
# 위 상한이 안쪽에서 먹지 않을 때만 닿는 마지막 방어선.
_BACKSTOP_GRACE_SECONDS = 5


class MonitoringWikiClient:
    """monitoring-wiki MCP 서버의 tool을 동기 인터페이스로 노출한다."""

    def __init__(self, url: str) -> None:
        self._url = url

    def ask(self, question: str) -> str:
        """증상 질문을 위키에 보내고 관련 패턴 컨텍스트를 반환한다.

        실패하거나 상한을 넘기면 예외를 던진다. 호출자가 gaps에 기록하고 계속 진행한다.
        """
        return self._call_tool("ask", {"question": question})

    def get_index(self) -> str:
        """위키 패턴 색인(마크다운 표)을 반환한다.

        실패하거나 상한을 넘기면 예외를 던진다. 호출자가 처리한다.
        """
        return self._call_tool("get_index", {})

    def get_pattern(self, pattern_id: str, sections: tuple[str, ...] = ()) -> str:
        """패턴 페이지 원문을 반환한다. sections를 주면 해당 절만 받는다.

        실패하거나 상한을 넘기면 예외를 던진다. 호출자가 처리한다.
        """
        arguments: dict = {"id": pattern_id}
        if sections:
            arguments["sections"] = list(sections)
        return self._call_tool("get_pattern", arguments)

    def _call_tool(self, name: str, arguments: dict) -> str:
        """MCP tool 하나를 동기로 호출한다.

        ``with ThreadPoolExecutor``를 쓰지 않는 이유: 블록을 나갈 때 작업 스레드가
        끝나기를 기다려서, 응답 없는 서버에서는 ``future.result(timeout=...)``이
        타임아웃을 던진 뒤에도 httpx 읽기 타임아웃(300초)까지 호출자를 붙잡는다.
        """
        pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="monitoring-wiki")
        try:
            future = pool.submit(
                asyncio.run, self._call_tool_within_deadline(name, arguments)
            )
            return future.result(timeout=_TIMEOUT_SECONDS + _BACKSTOP_GRACE_SECONDS)
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

    async def _call_tool_within_deadline(self, name: str, arguments: dict) -> str:
        return await asyncio.wait_for(
            self._call_tool_async(name, arguments), timeout=_TIMEOUT_SECONDS
        )

    async def _call_tool_async(self, name: str, arguments: dict) -> str:
        from mcp.client.streamable_http import streamable_http_client
        from mcp import ClientSession

        async with streamable_http_client(self._url) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                result = await s.call_tool(name, arguments)
        if result.is_error:
            raise RuntimeError(
                result.content[0].text if result.content else "MCP 오류"
            )
        return result.content[0].text if result.content else ""
