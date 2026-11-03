"""위키 조사 에이전트. 색인을 읽고 후보 페이지를 열어 대조한 뒤 맞는 패턴을 고른다."""

from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import Callable
from typing import Any

from langchain.agents import create_agent
from langchain.agents.middleware import AgentMiddleware
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage

from cluster_doctor.incident_analysis_agent.agent.runtime.llm_call_log import AgentRoleLogMiddleware
from cluster_doctor.incident_analysis_agent.agent.runtime.pseudonym import PseudonymizeMiddleware
from cluster_doctor.incident_analysis_agent.agent.runtime.rate_limit_retry import (
    RateLimitRetryMiddleware,
)
from cluster_doctor.incident_analysis_agent.agent.wiki_investigation.findings import (
    WikiFindings,
    WikiPattern,
)
from cluster_doctor.incident_analysis_agent.agent.wiki_investigation.prompt import (
    USER_TEMPLATE,
    WIKI_SYSTEM_PROMPT,
)
from cluster_doctor.incident_analysis_agent.agent.wiki_investigation.tools import (
    MAX_PATTERNS,
    WikiToolbox,
)

_logger = logging.getLogger(__name__)

# 조사 전체(모델 호출과 위키 호출을 합친 벽시계 시간)의 상한.
WIKI_TIME_BUDGET_SECONDS = 180
# 모델 호출과 도구 호출이 번갈아 한 걸음씩이다. 색인 1번과 패턴 3개에 최종 답을 더한 것에 여유를 둔다.
_RECURSION_LIMIT = 16
_REASON_MAX_CHARS = 200
_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE)

AgentRunner = Callable[[WikiToolbox, str], str]


class WikiTimeGuardMiddleware(AgentMiddleware):
    """조사 시간 상한을 넘기면 모델 호출을 막는다.

    상한을 넘긴 뒤 첫 모델 호출은 통과시켜 모델이 지금까지 연 패턴으로 최종 답을 쓰게 하고,
    그 뒤의 모델 호출은 ``TimeoutError``로 끊는다.
    """

    def __init__(self, deadline: float, clock: Callable[[], float] = time.monotonic) -> None:
        super().__init__()
        self._deadline = deadline
        self._clock = clock
        self._final_call_allowed = True

    def _admit(self) -> None:
        if self._clock() < self._deadline:
            return
        if not self._final_call_allowed:
            raise TimeoutError("위키 조사 시간 상한을 넘겼다")
        self._final_call_allowed = False

    def wrap_model_call(self, request: Any, handler: Callable[[Any], Any]) -> Any:
        self._admit()
        return handler(request)

    async def awrap_model_call(
        self, request: Any, handler: Callable[[Any], Any]
    ) -> Any:
        self._admit()
        return await handler(request)


def _text(content) -> str:
    if isinstance(content, str):
        return content
    return "".join(
        part.get("text", "") if isinstance(part, dict) else str(part) for part in content
    )


def make_agent_runner(model: BaseChatModel, *, recursion_limit: int = _RECURSION_LIMIT) -> AgentRunner:
    """도구 상자를 쥔 LangChain 에이전트를 돌리고 마지막 답 문장을 돌려주는 실행기."""

    def run(toolbox: WikiToolbox, analysis_context: str) -> str:
        agent = create_agent(
            model,
            toolbox.tools(),
            system_prompt=WIKI_SYSTEM_PROMPT,
            middleware=[
                WikiTimeGuardMiddleware(toolbox.deadline, toolbox.clock),
                AgentRoleLogMiddleware("wiki"),
                RateLimitRetryMiddleware(),
                PseudonymizeMiddleware(),
            ],
            name="wiki_agent",
        )
        final = agent.invoke(
            {"messages": [HumanMessage(content=USER_TEMPLATE.format(context=analysis_context))]},
            {"recursion_limit": recursion_limit},
        )
        return _text(final["messages"][-1].content)

    return run


def parse_picks(text: str) -> list[tuple[str, str]]:
    """최종 답의 JSON에서 (패턴 id, 이유)를 꺼낸다. 형식이 어긋나면 빈 목록."""
    try:
        data = json.loads(_FENCE.sub("", text.strip()))
    except (json.JSONDecodeError, TypeError):
        _logger.warning("[wiki_agent] 최종 답이 JSON이 아니라 고른 패턴을 버린다")
        return []
    items = data.get("patterns") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return []
    picks: list[tuple[str, str]] = []
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        pattern_id = str(item.get("id") or "").strip()
        if not pattern_id or pattern_id in seen:
            continue
        seen.add(pattern_id)
        picks.append((pattern_id, str(item.get("reason") or "").strip()[:_REASON_MAX_CHARS]))
    return picks[:MAX_PATTERNS]


class WikiInvestigator:
    def __init__(
        self,
        *,
        client,
        run_agent: AgentRunner,
        time_budget_seconds: float = WIKI_TIME_BUDGET_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client
        self._run_agent = run_agent
        self._time_budget_seconds = time_budget_seconds
        self._clock = clock

    def investigate(self, analysis_context: str) -> WikiFindings:
        """사건당 한 번 위키를 조사한다. 실패하면 빈 결과를 돌려주고 예외를 내보내지 않는다."""
        started = self._clock()
        toolbox = WikiToolbox(
            self._client, deadline=started + self._time_budget_seconds, clock=self._clock)
        try:
            answer = self._run_agent(toolbox, analysis_context)
        except Exception as exc:
            _logger.warning(
                "[wiki_agent] 조사가 실패해 위키 없이 종합한다: %s (%.1f초)",
                type(exc).__name__, self._clock() - started)
            return WikiFindings()
        patterns = tuple(
            WikiPattern(pattern_id=pattern_id, reason=reason, page=toolbox.pages[pattern_id])
            for pattern_id, reason in parse_picks(answer)
            if pattern_id in toolbox.pages
        )
        _logger.info(
            "[wiki_agent] 위키 호출 %d회, %.1f초, 연 패턴 %s, 고른 패턴 %s",
            toolbox.calls, self._clock() - started, list(toolbox.pages),
            [(pattern.pattern_id, pattern.reason) for pattern in patterns])
        return WikiFindings(patterns=patterns)
