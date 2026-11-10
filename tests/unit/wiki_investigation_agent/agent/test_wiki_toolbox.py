from cluster_doctor.wiki_investigation_agent.agent.index import index_pattern_ids
from cluster_doctor.wiki_investigation_agent.agent.tools import (
    WIKI_PAGE_SECTIONS,
    WikiToolbox,
)

INDEX = (
    "| id | 요약 |\n|---|---|\n"
    "| [heap-pressure](p/heap-pressure) | 힙 압박 |\n"
    "| [shard-unassigned](p/shard-unassigned) | 미할당 |\n"
    "| [gc-overhead](p/gc-overhead) | GC |\n"
    "| [disk-watermark](p/disk-watermark) | 디스크 |\n"
)


class FakeClient:
    """정해 둔 색인과 페이지를 돌려주고 호출을 기록하는 가짜 위키 클라이언트. test_wiki_investigator도 쓴다."""

    def __init__(self, *, index=INDEX, fail_pattern=False):
        self.index = index
        self.fail_pattern = fail_pattern
        self.index_calls = 0
        self.pattern_calls = []

    def get_index(self):
        self.index_calls += 1
        return self.index

    def get_pattern(self, pattern_id, sections=()):
        self.pattern_calls.append((pattern_id, sections))
        if self.fail_pattern:
            raise RuntimeError("down")
        return f"# {pattern_id}\n증상 본문"


class FakeClock:
    """``advance``로만 흐르는 시계."""

    def __init__(self, now=0.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def test_index_is_read_once_and_reused():
    client = FakeClient()
    toolbox = WikiToolbox(client)

    first = toolbox.get_index()
    second = toolbox.get_index()

    assert first == second
    assert client.index_calls == 1


def test_pattern_requires_the_index_first():
    client = FakeClient()
    toolbox = WikiToolbox(client)

    message = toolbox.get_pattern("heap-pressure")

    assert "get_index" in message
    assert client.pattern_calls == []


def test_unknown_id_never_reaches_the_wiki():
    client = FakeClient()
    toolbox = WikiToolbox(client)
    toolbox.get_index()

    message = toolbox.get_pattern("made-up")

    assert "색인" in message
    assert client.pattern_calls == []


def test_a_pattern_is_fetched_once_with_the_fixed_sections():
    client = FakeClient()
    toolbox = WikiToolbox(client)
    toolbox.get_index()

    toolbox.get_pattern("heap-pressure")
    toolbox.get_pattern("heap-pressure")

    assert client.pattern_calls == [("heap-pressure", WIKI_PAGE_SECTIONS)]
    assert list(toolbox.pages) == ["heap-pressure"]


def test_pattern_budget_stops_after_three_pages():
    client = FakeClient()
    toolbox = WikiToolbox(client)
    toolbox.get_index()
    for pattern_id in ("heap-pressure", "shard-unassigned", "gc-overhead"):
        toolbox.get_pattern(pattern_id)

    message = toolbox.get_pattern("disk-watermark")

    assert "최대 3개" in message
    assert len(client.pattern_calls) == 3
    assert "disk-watermark" not in toolbox.pages


def test_page_failure_stops_further_wiki_calls():
    client = FakeClient(fail_pattern=True)
    toolbox = WikiToolbox(client)
    toolbox.get_index()

    toolbox.get_pattern("heap-pressure")
    after = toolbox.get_pattern("gc-overhead")

    assert toolbox.failed is True
    assert "실패" in after
    assert len(client.pattern_calls) == 1


def test_index_without_pattern_ids_disables_the_wiki():
    toolbox = WikiToolbox(FakeClient(index="형식이 바뀐 색인"))

    message = toolbox.get_index()

    assert toolbox.failed is True
    assert "실패" in message


def test_calls_count_wiki_round_trips_only():
    client = FakeClient()
    toolbox = WikiToolbox(client)
    toolbox.get_index()
    toolbox.get_index()
    toolbox.get_pattern("made-up")
    toolbox.get_pattern("heap-pressure")
    toolbox.get_pattern("heap-pressure")

    assert toolbox.calls == 2


def test_pattern_is_refused_after_the_deadline_without_calling_the_wiki():
    client = FakeClient()
    clock = FakeClock()
    toolbox = WikiToolbox(client, deadline=10, clock=clock)
    toolbox.get_index()
    clock.now = 10

    message = toolbox.get_pattern("heap-pressure")

    assert "시간 상한" in message
    assert client.pattern_calls == []
    assert toolbox.pages == {}
    assert toolbox.failed is False


def test_index_is_refused_after_the_deadline_without_calling_the_wiki():
    client = FakeClient()
    clock = FakeClock(now=11)
    toolbox = WikiToolbox(client, deadline=10, clock=clock)

    message = toolbox.get_index()

    assert "시간 상한" in message
    assert client.index_calls == 0
    assert toolbox.calls == 0


def test_calls_before_the_deadline_still_reach_the_wiki():
    client = FakeClient()
    clock = FakeClock(now=9.9)
    toolbox = WikiToolbox(client, deadline=10, clock=clock)

    toolbox.get_index()
    toolbox.get_pattern("heap-pressure")

    assert client.index_calls == 1
    assert list(toolbox.pages) == ["heap-pressure"]


def test_already_read_index_and_pages_are_still_returned_after_the_deadline():
    client = FakeClient()
    clock = FakeClock()
    toolbox = WikiToolbox(client, deadline=10, clock=clock)
    index = toolbox.get_index()
    page = toolbox.get_pattern("heap-pressure")
    clock.now = 99

    assert toolbox.get_index() == index
    assert toolbox.get_pattern("heap-pressure") == page
    assert client.index_calls == 1
    assert len(client.pattern_calls) == 1


def test_toolbox_without_a_deadline_never_expires():
    client = FakeClient()
    clock = FakeClock()
    toolbox = WikiToolbox(client, clock=clock)
    toolbox.get_index()
    clock.now = 10**9

    toolbox.get_pattern("heap-pressure")

    assert list(toolbox.pages) == ["heap-pressure"]


def test_index_pattern_ids_reads_table_rows_in_order_once_and_ignores_other_lines():
    index = "\n".join([
        "# 색인", "",
        "| 패턴 | 요약 | 리소스 | 대표 시그니처 | confidence | last_seen |",
        "|---|---|---|---|---|---|",
        "| [massive-agg-query-timeout](patterns/massive-agg-query-timeout.md) | 요약 | query | master: <TS> | medium | 2026-10-22 |",
        "| [pat-b](patterns/pat-b.md) | 요약 | data | x | low | 2026-10-01 |",
        "| [massive-agg-query-timeout](patterns/massive-agg-query-timeout.md) | 중복 | | | | |",
        "본문 줄 [not-a-row](x.md)",
    ])

    assert index_pattern_ids(index) == ("massive-agg-query-timeout", "pat-b")
    assert index_pattern_ids("") == ()
