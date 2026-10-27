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
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport
from cluster_doctor.incident_analysis_agent.service.report_generation.incident_synthesis import (
    synthesize_incident,
)

START = datetime(2026, 10, 23, 18, 54, tzinfo=KST)
END = datetime(2026, 10, 23, 19, 4, tzinfo=KST)
SECTIONS = ("증상", "원인", "해결방법")
SUMMARY_PASSED = [{"claim_id": "summary", "status": "PASSED", "reason": "", "affected_evidence_refs": []}]


class FakeWriter:
    """draft_incident에 받은 인자를 기록하고 빈 초안을 돌려준다."""

    def __init__(self) -> None:
        self.kwargs: dict = {}

    def draft_incident(self, **kwargs) -> LogAnalysisReport:
        self.kwargs = kwargs
        return LogAnalysisReport(incident_id="inc-1", analyzed_from=START, analyzed_to=END)

    def revise_report(self, *args, **kwargs):
        return None


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


def _synthesize(wiki):
    writer = FakeWriter()
    llm = lambda messages, *, response_format=None: json.dumps(SUMMARY_PASSED)  # noqa: E731
    seams = SimpleNamespace(report_writer=writer, call_llm=llm,
                            metric_thresholds=DEFAULT_THRESHOLDS, wiki_client=wiki)
    report = synthesize_incident(
        seams=seams, incident=SimpleNamespace(incident_id="inc-1", cluster="es"),
        window_reports=[LogAnalysisReport(incident_id="inc-1", analyzed_from=START, analyzed_to=END)],
        observations=Observations(), evidence=[],
    )
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
