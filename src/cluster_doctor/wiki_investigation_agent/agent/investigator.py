"""위키 조사 에이전트. 색인을 읽고 후보 페이지를 열어 대조한 뒤 맞는 패턴을 고르고,
관측·구간 리포트·클러스터 구성 문서·패턴을 이어 원인 후보의 인과 사슬을 재구성한다."""

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
from cluster_doctor.wiki_investigation_agent.agent.findings import (
    ChainStep,
    Reconstruction,
    WikiFindings,
    WikiPattern,
    cut_at_line,
)
from cluster_doctor.wiki_investigation_agent.agent.prompt import (
    BASE_DATE_LINE,
    INFRA_SECTION_TITLE,
    USER_TEMPLATE,
    WIKI_SYSTEM_PROMPT,
    WINDOWS_SECTION_TITLE,
)
from cluster_doctor.wiki_investigation_agent.agent.tools import (
    MAX_PATTERNS,
    WikiToolbox,
)

_logger = logging.getLogger(__name__)

# 조사 전체(구성 문서 조회·모델 호출·위키 호출을 합친 벽시계 시간)의 상한.
# 패턴 고르기에 인과 재구성이 더해져 최종 답이 길어진 만큼 늘렸다.
WIKI_TIME_BUDGET_SECONDS = 240
# 모델 호출과 도구 호출이 번갈아 한 걸음씩이다. 색인 1번과 패턴 3개에 최종 답을 더한 것에 여유를 둔다.
_RECURSION_LIMIT = 16
_REASON_MAX_CHARS = 200
# 클러스터 구성 문서는 색인·페이지와 같은 크기로 자른다.
INFRA_MAX_CHARS = 6000
MAX_RECONSTRUCTIONS = 3
_STATEMENT_MAX_CHARS = 300
_STEP_MAX_CHARS = 300
_BASES = ("evidence", "infra", "wiki")
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


def _load_answer(text: str) -> dict | None:
    try:
        data = json.loads(_FENCE.sub("", text.strip()))
    except (json.JSONDecodeError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def compose_message(analysis_context: str, *, windows: str = "", infra: str = "", base_date: str = "") -> str:
    """모델에게 줄 사용자 메시지 본문. 추가 입력이 없으면 관측 JSON 그대로다."""
    parts = [analysis_context]
    if base_date:
        parts.append(BASE_DATE_LINE.format(base_date=base_date))
    if windows:
        parts.extend((WINDOWS_SECTION_TITLE, windows))
    if infra:
        parts.extend((INFRA_SECTION_TITLE, infra))
    return "\n\n".join(parts)


def parse_picks(text: str) -> list[tuple[str, str]]:
    """최종 답의 JSON에서 (패턴 id, 이유)를 꺼낸다. 형식이 어긋나면 빈 목록."""
    data = _load_answer(text)
    if data is None:
        _logger.warning("[wiki_agent] 최종 답이 JSON이 아니라 고른 패턴을 버린다")
        return []
    items = data.get("patterns")
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


def _clean_step(item, *, evidence_ids, pattern_ids, infra_loaded) -> ChainStep | None:
    """출처를 확인할 수 없는 고리는 버린다. 관측 고리는 이번 사건의 Evidence id가 남아야 한다."""
    if not isinstance(item, dict):
        return None
    step = str(item.get("step") or "").strip()[:_STEP_MAX_CHARS]
    basis = str(item.get("basis") or "").strip()
    raw_refs = item.get("refs") if isinstance(item.get("refs"), list) else []
    refs = tuple(dict.fromkeys(str(ref).strip() for ref in raw_refs if str(ref).strip()))
    if not step or basis not in _BASES:
        return None
    if basis == "evidence":
        refs = tuple(ref for ref in refs if ref in evidence_ids)
        return ChainStep(step, basis, refs) if refs else None
    if basis == "infra":
        return ChainStep(step, basis) if infra_loaded else None
    refs = tuple(ref for ref in refs if ref in pattern_ids)
    return ChainStep(step, basis, refs) if refs else None


def parse_reconstructions(text: str, *, evidence_ids, pattern_ids, infra_loaded: bool) -> tuple[Reconstruction, ...]:
    """최종 답의 재구성을 꺼낸다. 관측 고리가 하나도 남지 않은 사슬은 버린다."""
    data = _load_answer(text)
    items = data.get("reconstruction") if data is not None else None
    if not isinstance(items, list):
        return ()
    kept: list[Reconstruction] = []
    dropped = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        statement = str(item.get("statement") or "").strip()[:_STATEMENT_MAX_CHARS]
        raw_chain = item.get("chain") if isinstance(item.get("chain"), list) else []
        chain = tuple(step for step in (
            _clean_step(raw, evidence_ids=evidence_ids, pattern_ids=pattern_ids, infra_loaded=infra_loaded)
            for raw in raw_chain) if step is not None)
        if not statement or not any(step.basis == "evidence" for step in chain):
            dropped += 1
            continue
        wiki_ref = str(item.get("wiki_ref") or "").strip()
        kept.append(Reconstruction(statement, chain, wiki_ref if wiki_ref in pattern_ids else ""))
    if dropped:
        _logger.warning("[wiki_agent] 관측 근거가 남지 않은 재구성 %d개를 버린다", dropped)
    return tuple(kept[:MAX_RECONSTRUCTIONS])


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

    def _fetch_infra(self) -> str:
        """구성 문서는 모델이 고르지 않고 조사마다 한 번 가져온다. 실패하면 구성 고리 없이 조사한다."""
        try:
            raw = str(self._client.get_infra() or "").strip()
        except Exception as exc:
            _logger.warning("[wiki_agent] 클러스터 구성 문서를 가져오지 못해 구성 없이 조사한다: %s", type(exc).__name__)
            return ""
        text = cut_at_line(raw, INFRA_MAX_CHARS)
        if len(text) < len(raw):
            _logger.warning("클러스터 구성 문서(%d자)가 상한 %d자를 넘어 %d자까지만 싣는다.",
                            len(raw), INFRA_MAX_CHARS, len(text))
        return text

    def investigate(self, analysis_context: str, *, windows: str = "", base_date: str = "",
                    evidence_ids=()) -> WikiFindings:
        """사건당 한 번 위키를 조사한다. 실패하면 빈 결과를 돌려주고 예외를 내보내지 않는다.

        ``windows``는 구간 리포트 요약, ``base_date``는 티어 기준일, ``evidence_ids``는 이번 사건의 Evidence id다.
        재구성의 관측 고리는 ``evidence_ids``에 있는 id만 남는다.
        """
        started = self._clock()
        toolbox = WikiToolbox(
            self._client, deadline=started + self._time_budget_seconds, clock=self._clock)
        infra = self._fetch_infra()
        message = compose_message(analysis_context, windows=windows, infra=infra, base_date=base_date)
        try:
            answer = self._run_agent(toolbox, message)
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
        reconstructions = parse_reconstructions(
            answer, evidence_ids=frozenset(evidence_ids),
            pattern_ids=frozenset(pattern.pattern_id for pattern in patterns), infra_loaded=bool(infra))
        _logger.info(
            "[wiki_agent] 위키 호출 %d회, %.1f초, 연 패턴 %s, 고른 패턴 %s, 구성 문서 %s, 재구성 %d개",
            toolbox.calls, self._clock() - started, list(toolbox.pages),
            [(pattern.pattern_id, pattern.reason) for pattern in patterns],
            "있음" if infra else "없음", len(reconstructions))
        return WikiFindings(patterns=patterns, infra=infra, reconstructions=reconstructions)
