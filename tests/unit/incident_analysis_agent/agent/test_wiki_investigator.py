import json
import logging

import pytest
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from test_wiki_toolbox import FakeClient, FakeClock

from cluster_doctor.incident_analysis_agent.agent.wiki_investigation import findings as findings_module
from cluster_doctor.incident_analysis_agent.agent.wiki_investigation.findings import (
    WikiFindings,
    WikiPattern,
)
from cluster_doctor.incident_analysis_agent.agent.wiki_investigation.investigator import (
    WIKI_TIME_BUDGET_SECONDS,
    WikiInvestigator,
    WikiTimeGuardMiddleware,
    make_agent_runner,
    parse_picks,
)
from cluster_doctor.incident_analysis_agent.agent.wiki_investigation.prompt import (
    WIKI_SYSTEM_PROMPT,
)


def _answer(*picks):
    return json.dumps({"patterns": [{"id": i, "reason": r} for i, r in picks]}, ensure_ascii=False)


def test_only_opened_patterns_become_findings():
    def run(toolbox, context):
        toolbox.get_index()
        toolbox.get_pattern("heap-pressure")
        return _answer(("heap-pressure", "힙 압박 증상 일치"), ("gc-overhead", "열지 않은 패턴"))

    findings = WikiInvestigator(client=FakeClient(), run_agent=run).investigate("{}")

    assert findings.known_ids == ("heap-pressure",)
    assert findings.patterns[0].reason == "힙 압박 증상 일치"
    assert "증상 본문" in findings.patterns[0].page


def test_findings_are_empty_when_the_answer_is_not_json():
    def run(toolbox, context):
        toolbox.get_index()
        toolbox.get_pattern("heap-pressure")
        return "맞는 패턴이 있어 보인다"

    findings = WikiInvestigator(client=FakeClient(), run_agent=run).investigate("{}")

    assert findings.known_ids == ()


def test_findings_are_empty_when_the_runner_raises():
    def run(toolbox, context):
        raise RuntimeError("boom")

    findings = WikiInvestigator(client=FakeClient(), run_agent=run).investigate("{}")

    assert findings.known_ids == ()


def test_wiki_failure_leaves_no_patterns_even_if_the_answer_names_one():
    def run(toolbox, context):
        toolbox.get_index()
        toolbox.get_pattern("heap-pressure")
        return _answer(("heap-pressure", "일치"))

    findings = WikiInvestigator(client=FakeClient(fail_pattern=True), run_agent=run).investigate("{}")

    assert findings.known_ids == ()


def test_runner_receives_the_context_it_was_given():
    seen = {}

    def run(toolbox, context):
        seen["context"] = context
        return _answer()

    WikiInvestigator(client=FakeClient(), run_agent=run).investigate('{"onset_kst": "x"}')

    assert seen["context"] == '{"onset_kst": "x"}'


def test_parse_picks_reads_fenced_json_dedups_and_caps_reason():
    text = "```json\n" + _answer(("a", "r" * 300), ("a", "중복"), ("b", "")) + "\n```"

    picks = parse_picks(text)

    assert [pick[0] for pick in picks] == ["a", "b"]
    assert len(picks[0][1]) == 200


def test_parse_picks_caps_at_three_patterns():
    picks = parse_picks(_answer(("a", ""), ("b", ""), ("c", ""), ("d", "")))

    assert [pick[0] for pick in picks] == ["a", "b", "c"]


def test_prompt_block_carries_page_text_but_not_the_reason():
    findings = WikiFindings(patterns=(
        WikiPattern("heap-pressure", "힙 압박 증상 일치", "# heap-pressure\n증상 본문"),
        WikiPattern("gc-overhead", "GC 시간 증가가 일치", "# gc-overhead\nGC 본문"),
    ))

    block = findings.as_prompt_block()

    assert block == (
        "[패턴 heap-pressure]\n# heap-pressure\n증상 본문\n\n"
        "[패턴 gc-overhead]\n# gc-overhead\nGC 본문"
    )
    assert "힙 압박 증상 일치" not in block
    assert "GC 시간 증가가 일치" not in block
    assert "선택 이유" not in block


def test_prompt_block_is_empty_without_patterns():
    assert WikiFindings().as_prompt_block() == ""


def test_prompt_block_is_cut_at_a_line_and_warns_over_the_total_cap(caplog):
    page = ("Ω" * 99 + "\n") * 55
    findings = WikiFindings(patterns=tuple(
        WikiPattern(pattern_id, "이유", page) for pattern_id in ("pat-a", "pat-b", "pat-c")))

    with caplog.at_level(logging.WARNING):
        block = findings.as_prompt_block()

    assert 0 < len(block) <= findings_module.PAGES_MAX_CHARS
    assert not block.endswith("\n")
    assert "위키 패턴 페이지 합계" in caplog.text


def test_system_prompt_marks_the_reason_as_a_record_only_sentence():
    assert "선택 이유 한 문장(기록용)" in WIKI_SYSTEM_PROMPT


def test_log_line_names_opened_and_chosen_patterns_with_reasons(caplog):
    def run(toolbox, context):
        toolbox.get_index()
        toolbox.get_pattern("heap-pressure")
        toolbox.get_pattern("gc-overhead")
        return _answer(("heap-pressure", "힙 압박 증상 일치"))

    with caplog.at_level(logging.INFO):
        WikiInvestigator(client=FakeClient(), run_agent=run).investigate("{}")

    line = next(r.getMessage() for r in caplog.records if "위키 호출" in r.getMessage())
    assert "위키 호출 3회" in line
    assert "['heap-pressure', 'gc-overhead']" in line
    assert "[('heap-pressure', '힙 압박 증상 일치')]" in line


def test_time_budget_defaults_to_three_minutes_and_sets_the_toolbox_deadline():
    seen = {}

    def run(toolbox, context):
        seen["deadline"] = toolbox.deadline
        seen["clock"] = toolbox.clock
        return _answer()

    clock = FakeClock(now=1000)
    WikiInvestigator(client=FakeClient(), run_agent=run, clock=clock).investigate("{}")

    assert WIKI_TIME_BUDGET_SECONDS == 180
    assert seen["deadline"] == 1180
    assert seen["clock"] is clock


def test_time_budget_can_be_overridden():
    seen = {}

    def run(toolbox, context):
        seen["deadline"] = toolbox.deadline
        return _answer()

    WikiInvestigator(
        client=FakeClient(), run_agent=run, time_budget_seconds=30, clock=FakeClock(now=5),
    ).investigate("{}")

    assert seen["deadline"] == 35


class _Handler:
    def __init__(self):
        self.calls = 0

    def __call__(self, request):
        self.calls += 1
        return "answer"


def test_guard_passes_every_call_before_the_deadline():
    clock = FakeClock()
    guard = WikiTimeGuardMiddleware(10, clock)
    handler = _Handler()

    for _ in range(5):
        assert guard.wrap_model_call("request", handler) == "answer"

    assert handler.calls == 5


def test_guard_allows_exactly_one_model_call_after_the_deadline_then_raises():
    clock = FakeClock()
    guard = WikiTimeGuardMiddleware(10, clock)
    handler = _Handler()
    guard.wrap_model_call("request", handler)
    clock.now = 10

    assert guard.wrap_model_call("request", handler) == "answer"
    with pytest.raises(TimeoutError):
        guard.wrap_model_call("request", handler)
    with pytest.raises(TimeoutError):
        guard.wrap_model_call("request", handler)

    assert handler.calls == 2


async def test_async_guard_allows_exactly_one_model_call_after_the_deadline_then_raises():
    clock = FakeClock(now=11)
    guard = WikiTimeGuardMiddleware(10, clock)
    calls = []

    async def handler(request):
        calls.append(request)
        return "answer"

    assert await guard.awrap_model_call("request", handler) == "answer"
    with pytest.raises(TimeoutError):
        await guard.awrap_model_call("request", handler)

    assert len(calls) == 1


def test_findings_are_empty_when_the_guard_stops_the_model(caplog):
    clock = FakeClock()

    def run(toolbox, context):
        guard = WikiTimeGuardMiddleware(toolbox.deadline, toolbox.clock)
        toolbox.get_index()
        toolbox.get_pattern("heap-pressure")
        clock.advance(WIKI_TIME_BUDGET_SECONDS)
        guard.wrap_model_call("request", _Handler())
        guard.wrap_model_call("request", _Handler())
        return _answer(("heap-pressure", "일치"))

    with caplog.at_level(logging.WARNING):
        findings = WikiInvestigator(client=FakeClient(), run_agent=run, clock=clock).investigate("{}")

    assert findings.known_ids == ()
    assert "TimeoutError" in caplog.text


class ScriptedModel(BaseChatModel):
    """미리 정한 응답을 차례로 돌려주는 가짜 모델. 도구 호출을 낼 수 있다."""

    replies: list
    cursor: int = 0

    @property
    def _llm_type(self) -> str:
        return "scripted"

    def bind_tools(self, tools, **kwargs):
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        reply = self.replies[self.cursor]
        self.cursor += 1
        return ChatResult(generations=[ChatGeneration(message=reply)])


def _tool_call(name, args, call_id):
    return AIMessage(content="", tool_calls=[{"name": name, "args": args, "id": call_id}])


class TickingClient(FakeClient):
    """위키 호출 한 번마다 시계를 ``cost``초 흐르게 하는 가짜 위키."""

    def __init__(self, clock, cost):
        super().__init__()
        self._clock = clock
        self._cost = cost

    def get_pattern(self, pattern_id, sections=()):
        self._clock.advance(self._cost)
        return super().get_pattern(pattern_id, sections)


def test_langchain_runner_runs_the_tool_loop_end_to_end():
    model = ScriptedModel(replies=[
        _tool_call("get_index", {}, "c1"),
        _tool_call("get_pattern", {"pattern_id": "heap-pressure"}, "c2"),
        AIMessage(content=_answer(("heap-pressure", "힙 압박 증상 일치"))),
    ])
    client = FakeClient()

    findings = WikiInvestigator(client=client, run_agent=make_agent_runner(model)).investigate("{}")

    assert findings.known_ids == ("heap-pressure",)
    assert client.index_calls == 1
    assert len(client.pattern_calls) == 1
    assert model.cursor == 3


def test_langchain_runner_lets_the_model_write_its_answer_after_the_deadline():
    clock = FakeClock()
    model = ScriptedModel(replies=[
        _tool_call("get_index", {}, "c1"),
        _tool_call("get_pattern", {"pattern_id": "heap-pressure"}, "c2"),
        AIMessage(content=_answer(("heap-pressure", "힙 압박 증상 일치"))),
    ])
    client = TickingClient(clock, cost=WIKI_TIME_BUDGET_SECONDS + 1)

    findings = WikiInvestigator(
        client=client, run_agent=make_agent_runner(model), clock=clock).investigate("{}")

    assert findings.known_ids == ("heap-pressure",)
    assert model.cursor == 3


def test_langchain_runner_stops_a_model_that_keeps_calling_tools_after_the_deadline():
    clock = FakeClock()
    model = ScriptedModel(replies=[
        _tool_call("get_index", {}, "c1"),
        _tool_call("get_pattern", {"pattern_id": "heap-pressure"}, "c2"),
        _tool_call("get_pattern", {"pattern_id": "gc-overhead"}, "c3"),
        AIMessage(content=_answer(("heap-pressure", "힙 압박 증상 일치"))),
    ])
    client = TickingClient(clock, cost=WIKI_TIME_BUDGET_SECONDS + 1)

    findings = WikiInvestigator(
        client=client, run_agent=make_agent_runner(model), clock=clock).investigate("{}")

    assert findings.known_ids == ()
    assert model.cursor == 3
    assert [call[0] for call in client.pattern_calls] == ["heap-pressure"]
