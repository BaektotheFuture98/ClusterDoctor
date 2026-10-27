"""사건 종합이 위키 색인과 페이지 열기 함수를 draft_incident에 넘기는지 검증한다.

writer·LLM·위키는 모두 가짜다. 실제 위키 서버나 LLM을 부르지 않는다.
"""

import json
import logging
from datetime import datetime
from types import SimpleNamespace

from cluster_doctor.incident_analysis_agent.datasource.clickhouse.node_metric import DEFAULT_THRESHOLDS
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport, ReportRecommendation
from cluster_doctor.incident_analysis_agent.service.report_generation.incident_synthesis import (
    synthesize_incident,
)

START = datetime(2026, 10, 23, 18, 54, tzinfo=KST)
END = datetime(2026, 10, 23, 19, 4, tzinfo=KST)
SECTIONS = ("증상", "원인", "해결방법")
SUMMARY_PASSED = [{"claim_id": "summary", "status": "PASSED", "reason": "", "affected_evidence_refs": []}]


class FakeWriter:
    """draft_incident에 받은 인자를 기록하고, 정해 둔 wiki_ref를 단 조치가 있는 초안을 돌려준다."""

    def __init__(self, wiki_refs: tuple[str, ...] = (), revised_wiki_refs: tuple[str, ...] | None = None) -> None:
        self.kwargs: dict = {}
        self.wiki_refs = wiki_refs
        self.revised_wiki_refs = revised_wiki_refs
        self.revise_calls = 0

    @staticmethod
    def _report(wiki_refs: tuple[str, ...], cause_index: int | None = None) -> LogAnalysisReport:
        return LogAnalysisReport(
            incident_id="inc-1", analyzed_from=START, analyzed_to=END,
            recommendations=tuple(
                ReportRecommendation(text=f"조치 {n}", wiki_ref=ref, cause_index=cause_index)
                for n, ref in enumerate(wiki_refs)
            ),
        )

    def draft_incident(self, **kwargs) -> LogAnalysisReport:
        self.kwargs = kwargs
        # 재수정을 지정하면 원인 범위를 벗어난 cause_index를 달아 구조 검증이 재수정을 요구하게 한다.
        return self._report(self.wiki_refs, cause_index=0 if self.revised_wiki_refs is not None else None)

    def revise_report(self, *args, **kwargs):
        self.revise_calls += 1
        return None if self.revised_wiki_refs is None else self._report(self.revised_wiki_refs)


class FakeWiki:
    def __init__(self, index: str = "| 패턴 |\n|---|\n| [a](patterns/a.md) |", index_error=None,
                 failing_ids: tuple[str, ...] = ()) -> None:
        self.index = index
        self.index_error = index_error
        self.failing_ids = failing_ids
        self.index_calls = 0
        self.pattern_calls: list[tuple[str, tuple[str, ...]]] = []

    def get_index(self) -> str:
        self.index_calls += 1
        if self.index_error is not None:
            raise self.index_error
        return self.index

    def get_pattern(self, pattern_id: str, sections: tuple[str, ...] = ()) -> str:
        self.pattern_calls.append((pattern_id, sections))
        if pattern_id in self.failing_ids:
            raise RuntimeError(f"{pattern_id} 조회 실패")
        return f"페이지 {pattern_id}"


def _run(wiki, writer: FakeWriter) -> LogAnalysisReport:
    llm = lambda messages, *, response_format=None: json.dumps(SUMMARY_PASSED)  # noqa: E731
    seams = SimpleNamespace(report_writer=writer, call_llm=llm,
                            metric_thresholds=DEFAULT_THRESHOLDS, wiki_client=wiki)
    return synthesize_incident(
        seams=seams, incident=SimpleNamespace(incident_id="inc-1", cluster="es"),
        window_reports=[LogAnalysisReport(incident_id="inc-1", analyzed_from=START, analyzed_to=END)],
        observations=Observations(), evidence=[],
    )


def _synthesize(wiki, wiki_refs: tuple[str, ...] = ()):
    writer = FakeWriter(wiki_refs)
    report = _run(wiki, writer)
    return writer.kwargs, report


def test_without_a_wiki_client_nothing_is_passed():
    kwargs, _ = _synthesize(None)

    assert kwargs["wiki_index"] == ""
    assert kwargs["open_wiki_pages"] is None
    assert "wiki_context" not in kwargs


def test_index_is_read_once_and_pages_open_in_order_with_sections():
    wiki = FakeWiki(index="색인 본문")

    kwargs, _ = _synthesize(wiki)

    assert kwargs["wiki_index"] == "색인 본문"
    assert wiki.index_calls == 1
    assert wiki.pattern_calls == []

    pages = kwargs["open_wiki_pages"](("a", "b"))

    assert wiki.pattern_calls == [("a", SECTIONS), ("b", SECTIONS)]
    assert pages == {"a": "페이지 a", "b": "페이지 b"}
    assert wiki.index_calls == 1


def test_index_failure_falls_back_to_no_wiki_and_logs_a_warning(caplog):
    wiki = FakeWiki(index_error=RuntimeError("wiki down"))

    with caplog.at_level(logging.WARNING):
        kwargs, _ = _synthesize(wiki)

    assert kwargs["wiki_index"] == ""
    assert kwargs["open_wiki_pages"] is None
    assert "위키 색인 조회가 실패" in caplog.text


def test_blank_index_falls_back_to_no_wiki():
    kwargs, _ = _synthesize(FakeWiki(index="  \n "))

    assert kwargs["wiki_index"] == ""
    assert kwargs["open_wiki_pages"] is None


def test_a_failing_page_is_skipped_and_the_others_still_open(caplog):
    wiki = FakeWiki(failing_ids=("a",))
    kwargs, _ = _synthesize(wiki)

    with caplog.at_level(logging.WARNING):
        pages = kwargs["open_wiki_pages"](("a", "b"))

    assert pages == {"b": "페이지 b"}
    assert [call[0] for call in wiki.pattern_calls] == ["a", "b"]
    assert "a" in caplog.text


def test_wiki_failure_does_not_change_the_verification_status():
    _, with_wiki = _synthesize(FakeWiki())
    _, failed = _synthesize(FakeWiki(index_error=RuntimeError("down")))
    _, without = _synthesize(None)

    assert with_wiki.verification_status == failed.verification_status == without.verification_status


def test_final_report_keeps_only_wiki_refs_that_are_in_the_index():
    _, report = _synthesize(FakeWiki(), wiki_refs=("a", "지어낸 패턴", " a ", ""))

    assert [item.wiki_ref for item in report.recommendations] == ["a", "", "a", ""]


def test_every_wiki_ref_is_cleared_when_there_is_no_wiki():
    _, report = _synthesize(None, wiki_refs=("a",))

    assert report.recommendations[0].wiki_ref == ""


def test_wiki_ref_rewritten_by_a_revision_is_limited_to_the_index_again():
    writer = FakeWriter(wiki_refs=("a",), revised_wiki_refs=("지어낸 패턴", "a"))

    report = _run(FakeWiki(), writer)

    assert writer.revise_calls >= 1
    assert [item.wiki_ref for item in report.recommendations] == ["", "a"]
