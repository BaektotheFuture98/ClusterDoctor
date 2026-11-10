"""위키 조사 에이전트가 쓰는 도구 두 개와 그 호출 상한.

모델에게는 id만 받게 하고 sections는 고정한다. 색인에 없는 id는 위키로 보내지 않는다.
위키가 한 번 실패하면 이후 호출은 위키로 나가지 않고 실패를 알린다.
조사 전체의 시간 상한(``deadline``)을 넘기면 이미 읽은 색인·연 페이지만 돌려주고 위키로 나가지 않는다.
"""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable

from langchain_core.tools import BaseTool, StructuredTool

from cluster_doctor.wiki_investigation_agent.agent.findings import cut_at_line
from cluster_doctor.wiki_investigation_agent.agent.index import index_pattern_ids

_logger = logging.getLogger(__name__)

WIKI_PAGE_SECTIONS = ("증상", "원인", "해결방법")
MAX_PATTERNS = 3
INDEX_MAX_CHARS = 6000
PAGE_MAX_CHARS = 6000

_FAILED = "위키 조회가 실패했다. 위키 없이 결론을 내라."
_TIME_UP = "조사 시간 상한을 넘겨 위키를 더 열지 않는다. 지금까지 연 패턴으로 결론을 내라."


class WikiToolbox:
    def __init__(
        self,
        client,
        *,
        max_patterns: int = MAX_PATTERNS,
        index_max_chars: int = INDEX_MAX_CHARS,
        page_max_chars: int = PAGE_MAX_CHARS,
        deadline: float = math.inf,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client
        self._max_patterns = max_patterns
        self._index_max_chars = index_max_chars
        self._page_max_chars = page_max_chars
        self._index_text = ""
        self._index_ids: tuple[str, ...] = ()
        self.deadline = deadline
        self.clock = clock
        self.pages: dict[str, str] = {}
        self.failed = False
        self.calls = 0

    def _time_is_up(self) -> bool:
        if self.clock() < self.deadline:
            return False
        _logger.warning("[wiki_agent] 조사 시간 상한을 넘겨 위키 호출을 막는다")
        return True

    def get_index(self) -> str:
        if self.failed:
            return _FAILED
        if self._index_text:
            return self._index_text
        if self._time_is_up():
            return _TIME_UP
        self.calls += 1
        try:
            raw = self._client.get_index()
        except Exception as exc:
            _logger.warning("[wiki_agent] 색인 조회가 실패해 위키를 끈다: %s", exc)
            self.failed = True
            return _FAILED
        text = cut_at_line(raw.strip(), self._index_max_chars)
        if len(text) < len(raw.strip()):
            _logger.warning(
                "위키 색인(%d자)이 상한 %d자를 넘어 %d자까지만 싣는다. 뒤쪽 패턴은 모델에게 보이지 않는다.",
                len(raw.strip()), self._index_max_chars, len(text))
        ids = index_pattern_ids(text)
        if not ids:
            _logger.warning("[wiki_agent] 위키 색인에서 패턴 id를 읽지 못해 위키를 끈다. 색인 형식이 바뀌었는지 확인한다.")
            self.failed = True
            return _FAILED
        self._index_text, self._index_ids = text, ids
        return text

    def get_pattern(self, pattern_id: str) -> str:
        pattern_id = pattern_id.strip()
        if self.failed:
            return _FAILED
        if pattern_id in self.pages:
            return self.pages[pattern_id]
        if self._time_is_up():
            return _TIME_UP
        if not self._index_ids:
            return "먼저 get_index로 색인을 읽어라."
        if pattern_id not in self._index_ids:
            return "색인에 없는 id다. 색인의 패턴 id를 그대로 써라."
        if len(self.pages) >= self._max_patterns:
            return f"패턴은 최대 {self._max_patterns}개까지 열 수 있다. 지금까지 연 패턴으로 결론을 내라."
        self.calls += 1
        try:
            page = self._client.get_pattern(pattern_id, sections=WIKI_PAGE_SECTIONS)
        except Exception as exc:
            _logger.warning("[wiki_agent] 패턴 %s 페이지를 열지 못해 위키를 끈다: %s", pattern_id, exc)
            self.failed = True
            return _FAILED
        text = cut_at_line(page.strip(), self._page_max_chars)
        if len(text) < len(page.strip()):
            _logger.warning(
                "위키 패턴 %s 페이지(%d자)가 상한 %d자를 넘어 %d자까지만 싣는다.",
                pattern_id, len(page.strip()), self._page_max_chars, len(text))
        self.pages[pattern_id] = text
        return text

    def tools(self) -> list[BaseTool]:
        return [
            StructuredTool.from_function(
                self.get_index,
                name="get_index",
                description="위키 장애 패턴 색인(표)을 읽는다. 인자가 없다. 먼저 한 번 호출한다.",
            ),
            StructuredTool.from_function(
                self.get_pattern,
                name="get_pattern",
                description=(
                    f"색인의 패턴 id 하나를 열어 증상·원인·해결방법을 읽는다. 최대 {self._max_patterns}개까지 열 수 있다."
                ),
            ),
        ]
