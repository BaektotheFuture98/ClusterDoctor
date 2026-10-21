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

_TIMEOUT_SECONDS = 30


class MonitoringWikiClient:
    """monitoring-wiki MCP 서버의 ask tool을 동기 인터페이스로 노출한다."""

    def __init__(self, url: str) -> None:
        self._url = url

    def ask(self, question: str) -> str:
        """증상 질문을 위키에 보내고 관련 패턴 컨텍스트를 반환한다.

        실패하면 빈 문자열을 반환한다. 호출자는 gaps에 기록하고 계속 진행한다.
        """
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, self._ask(question))
            return future.result(timeout=_TIMEOUT_SECONDS)

    async def _ask(self, question: str) -> str:
        from mcp.client.streamable_http import streamable_http_client
        from mcp import ClientSession

        async with streamable_http_client(self._url) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                result = await asyncio.wait_for(
                    s.call_tool("ask", {"question": question}),
                    timeout=_TIMEOUT_SECONDS - 2,
                )
        if result.is_error:
            raise RuntimeError(
                result.content[0].text if result.content else "MCP 오류"
            )
        return result.content[0].text if result.content else ""
