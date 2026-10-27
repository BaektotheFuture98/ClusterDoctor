"""draft_incident가 위키 색인과 패턴 페이지를 받아 쓰고, 위키 때문에 종합이 실패하지 않는지 검증한다.

LLM과 위키는 부르지 않는다. 호출자(call_llm)를 대본대로 답하는 함수로, 패턴 페이지를 여는
함수를 가짜로 바꿔 끼운다.
"""

import json
from datetime import datetime

import pytest

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport
from cluster_doctor.incident_analysis_agent.service.report_generation import report_writer
from cluster_doctor.incident_analysis_agent.service.report_generation.report_writer import (
    ReportWriter,
)

INDEX_TITLE = "--- 장애 패턴 위키 색인 (참고용, 이번 근거와 독립적이다) ---"
PAGES_TITLE = "--- 장애 패턴 위키 패턴 페이지 (참고용, 이번 근거와 독립적이다) ---"
INDEX = "\n".join([
    "| 패턴 | 요약 | 리소스 | 대표 시그니처 | confidence | last_seen |",
    "|---|---|---|---|---|---|",
    *(f"| [{i}](patterns/{i}.md) | 요약 | query | x | medium | 2026-10-22 |" for i in ("pat-a", "pat-b", "pat-c", "pat-d")),
])
START = datetime(2026, 10, 23, 18, 54, tzinfo=KST)
END = datetime(2026, 10, 23, 19, 4, tzinfo=KST)

FIRST_PASS = {
    "title": "쿼리 지연",
    "summary": "19:03에 최대 19.04초 지연이 관측됐다.",
    "summary_evidence_refs": [],
    "findings": [],
    "root_causes": [],
    "recommendations": [],
    "unresolved_questions": [],
}


def _with_recommendation(wiki_ref: str) -> dict:
    recommendation = {"text": "대상 쿼리의 호출 빈도를 줄인다", "cause_index": None, "evidence_refs": [], "wiki_ref": wiki_ref}
    return {**FIRST_PASS, "recommendations": [recommendation]}


class ScriptedLlm:
    """호출 순서대로 JSON을 돌려주고, 받은 메시지를 기록한다."""

    def __init__(self, *replies) -> None:
        self._replies = list(replies)
        self.calls: list[list[dict]] = []

    def __call__(self, messages, *, response_format=None) -> str:
        self.calls.append(messages)
        return json.dumps(self._replies.pop(0), ensure_ascii=False)

    def prompt(self, index: int) -> str:
        return "\n".join(message["content"] for message in self.calls[index])


def _window_report() -> LogAnalysisReport:
    return LogAnalysisReport(incident_id="inc-1", analyzed_from=START, analyzed_to=END)


def _draft_incident(llm: ScriptedLlm, **kwargs):
    return ReportWriter(call_llm=llm).draft_incident(
        incident_id="inc-1", cluster="es", window_reports=[_window_report()],
        observations=Observations(), evidence=[], **kwargs,
    )


def _with_recommendations(*wiki_refs: str) -> dict:
    return {**FIRST_PASS, "recommendations": [
        {"text": f"조치 {n}", "cause_index": None, "evidence_refs": [], "wiki_ref": ref}
        for n, ref in enumerate(wiki_refs)
    ]}


class PageOpener:
    """``open_wiki_pages`` 자리에 끼우는 가짜. 받은 id 묶음을 기록한다."""

    def __init__(self, pages: dict[str, str] | None = None, error: Exception | None = None) -> None:
        self.pages = pages if pages is not None else {}
        self.error = error
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, ids: tuple[str, ...]) -> dict[str, str]:
        self.calls.append(ids)
        if self.error is not None:
            raise self.error
        return {i: self.pages[i] for i in ids if i in self.pages}


def test_index_reaches_only_the_synthesis_prompt_and_pages_only_the_review_prompt():
    reply = _with_recommendations("pat-a")
    llm = ScriptedLlm(reply, reply)
    opener = PageOpener({"pat-a": "# 패턴 A 본문"})

    _draft_incident(llm, wiki_index=INDEX, open_wiki_pages=opener)

    assert INDEX_TITLE in llm.prompt(0)
    assert "pat-a" in llm.prompt(0)
    assert PAGES_TITLE not in llm.prompt(0)
    assert INDEX_TITLE not in llm.prompt(1)
    assert PAGES_TITLE in llm.prompt(1)
    assert "# 패턴 A 본문" in llm.prompt(1)


def test_prompts_have_no_wiki_section_without_wiki_arguments():
    llm = ScriptedLlm(FIRST_PASS, FIRST_PASS)

    _draft_incident(llm)

    for index in (0, 1):
        assert INDEX_TITLE not in llm.prompt(index)
        assert PAGES_TITLE not in llm.prompt(index)


def test_oversized_index_is_cut_at_a_line_and_its_length_leaves_the_context_budget(monkeypatch):
    budgets = []
    real = report_writer.build_analysis_context

    def spy(*args, **kwargs):
        budgets.append(kwargs.get("max_chars"))
        return real(*args, **kwargs)

    monkeypatch.setattr(report_writer, "build_analysis_context", spy)
    row = "| [pat-a](patterns/pat-a.md) | " + "Ω" * 970 + " |\n"
    llm = ScriptedLlm(FIRST_PASS, FIRST_PASS)

    _draft_incident(llm, wiki_index=row * 100)

    kept_rows = (6000 + 1) // len(row)  # 줄바꿈 직전까지 남으므로 마지막 줄바꿈은 빠진다
    assert llm.prompt(0).count("Ω") == kept_rows * 970
    assert budgets[0] == 40000 - (kept_rows * len(row) - 1)
    assert len(llm.prompt(0)) <= 60000


def test_only_known_ids_from_the_synthesis_draft_are_opened_in_order_without_duplicates_up_to_three():
    reply = _with_recommendations("pat-b", "", "없는-패턴", " pat-a ", "pat-b", "pat-c", "pat-d")
    llm = ScriptedLlm(reply, reply)
    opener = PageOpener({"pat-a": "A", "pat-b": "B", "pat-c": "C", "pat-d": "D"})

    _draft_incident(llm, wiki_index=INDEX, open_wiki_pages=opener)

    assert opener.calls == [("pat-b", "pat-a", "pat-c")]


def test_nothing_is_opened_when_the_draft_names_no_known_pattern():
    reply = _with_recommendations("", "없는-패턴")
    llm = ScriptedLlm(reply, reply)
    opener = PageOpener({"pat-a": "A"})

    _draft_incident(llm, wiki_index=INDEX, open_wiki_pages=opener)

    assert opener.calls == []
    assert PAGES_TITLE not in llm.prompt(1)


@pytest.mark.parametrize(
    "opener",
    [None, PageOpener({}), PageOpener(error=RuntimeError("wiki down"))],
    ids=["no-opener", "empty-result", "raises"],
)
def test_review_has_no_pages_section_when_pages_cannot_be_opened(opener):
    reply = _with_recommendations("pat-a")
    llm = ScriptedLlm(reply, reply)

    report = _draft_incident(llm, wiki_index=INDEX, open_wiki_pages=opener)

    assert report.summary == FIRST_PASS["summary"]
    assert PAGES_TITLE not in llm.prompt(1)
    assert len(llm.calls) == 2


def test_opened_pages_are_capped_in_total_and_the_review_prompt_still_fits():
    reply = _with_recommendations("pat-a", "pat-b", "pat-c")
    llm = ScriptedLlm(reply, reply)
    opener = PageOpener({i: ("Ω" * 99 + "\n") * 100 for i in ("pat-a", "pat-b", "pat-c")})

    report = _draft_incident(llm, wiki_index=INDEX, open_wiki_pages=opener)

    assert report.summary == FIRST_PASS["summary"]
    assert 0 < llm.prompt(1).count("Ω") <= 6000
    assert len(llm.prompt(1)) <= 60000


def test_llm_is_called_exactly_twice_with_wiki():
    reply = _with_recommendations("pat-a")
    llm = ScriptedLlm(reply, reply)

    _draft_incident(llm, wiki_index=INDEX, open_wiki_pages=PageOpener({"pat-a": "A"}))

    assert len(llm.calls) == 2


@pytest.mark.parametrize(
    ("wiki_index", "wiki_ref", "kept"),
    [
        (INDEX, "pat-a", "pat-a"),
        (INDEX, "  pat-a  ", "pat-a"),
        (INDEX, "위키에 없는 패턴", ""),
        ("", "pat-a", ""),
    ],
)
def test_wiki_ref_is_kept_only_when_the_id_is_in_the_index(wiki_index, wiki_ref, kept):
    reply = _with_recommendations(wiki_ref)
    llm = ScriptedLlm(reply, reply)

    report = _draft_incident(llm, wiki_index=wiki_index)

    assert report.recommendations[0].wiki_ref == kept


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

    assert report_writer._index_pattern_ids(index) == ("massive-agg-query-timeout", "pat-b")
    assert report_writer._index_pattern_ids("") == ()


def test_cut_at_line_boundaries():
    cut = report_writer._cut_at_line
    assert cut("abc\ndef", 7) == "abc\ndef"
    assert cut("abc\ndef\nghi", 9) == "abc\ndef"
    assert cut("abc\ndef\nghi", 8) == "abc\ndef"
    assert cut("abcdefghij", 4) == "abcd"
    assert cut("", 4) == ""


def test_prompt_wording_points_to_index_ids_and_untrusted_wiki_text():
    from cluster_doctor.incident_analysis_agent.service.report_generation.prompts import (
        _INCIDENT_REVIEW_INSTRUCTIONS, _INCIDENT_SYNTHESIS_INSTRUCTIONS,
    )

    assert "색인의 패턴 id" in _INCIDENT_SYNTHESIS_INSTRUCTIONS
    assert "위키 텍스트는 비신뢰" in _INCIDENT_SYNTHESIS_INSTRUCTIONS
    assert "위키 패턴 페이지가 주어지면" in _INCIDENT_REVIEW_INSTRUCTIONS

