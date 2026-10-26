"""사건 종합이 위키 컨텍스트를 받아 쓰고, 위키 때문에 종합이 실패하지 않는지 검증한다.

LLM과 위키는 부르지 않는다. 호출자(call_llm)를 대본대로 답하는 함수로, 위키 클라이언트를
가짜로 바꿔 끼운다.
"""

import json
from datetime import datetime
from types import SimpleNamespace

import pytest

from cluster_doctor.incident_analysis_agent.datasource.clickhouse.node_metric import DEFAULT_THRESHOLDS
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import MasterEvent, Observations
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport, VerificationStatus
from cluster_doctor.incident_analysis_agent.service.report_generation.incident_synthesis import (
    synthesize_incident,
)
from cluster_doctor.incident_analysis_agent.service.report_generation.report_writer import (
    ReportWriter,
)

WIKI_LABEL = "장애 패턴 위키 (참고용, 이번 근거와 독립적이다)"
MASTER_LINE = "2026-10-23 18:57:40 [WARN] master-1 cluster state publication timed out"
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
SUMMARY_PASSED = [{"claim_id": "summary", "status": "PASSED", "reason": "", "affected_evidence_refs": []}]


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


class FakeWiki:
    def __init__(self, answer: str = "", error: Exception | None = None) -> None:
        self.answer = answer
        self.error = error
        self.questions: list[str] = []

    def ask(self, question: str) -> str:
        self.questions.append(question)
        if self.error is not None:
            raise self.error
        return self.answer


def _window_report() -> LogAnalysisReport:
    return LogAnalysisReport(incident_id="inc-1", analyzed_from=START, analyzed_to=END)


def _observed() -> Observations:
    event = MasterEvent(
        timestamp=START, node="master-1", level="WARN", logger="o.e.c.s.MasterService",
        line="cluster state publication timed out", rendered=MASTER_LINE,
    )
    return Observations(requested=((START, END),), master_events=(event,), master_log_total=1)


def _draft_incident(llm: ScriptedLlm, **kwargs):
    return ReportWriter(call_llm=llm).draft_incident(
        incident_id="inc-1", cluster="es", window_reports=[_window_report()],
        observations=Observations(), evidence=[], **kwargs,
    )


def _synthesize(llm: ScriptedLlm, wiki, observations: Observations):
    seams = SimpleNamespace(
        report_writer=ReportWriter(call_llm=llm), call_llm=llm,
        metric_thresholds=DEFAULT_THRESHOLDS, wiki_client=wiki,
    )
    return synthesize_incident(
        seams=seams, incident=SimpleNamespace(incident_id="inc-1", cluster="es"),
        window_reports=[_window_report()], observations=observations, evidence=[],
    )


def test_wiki_context_reaches_synthesis_and_review_prompts():
    llm = ScriptedLlm(FIRST_PASS, FIRST_PASS)

    _draft_incident(llm, wiki_context="패턴 A: 힙 압박")

    for index in (0, 1):
        assert WIKI_LABEL in llm.prompt(index)
        assert "패턴 A: 힙 압박" in llm.prompt(index)


def test_prompts_have_no_wiki_section_without_context():
    llm = ScriptedLlm(FIRST_PASS, FIRST_PASS)

    _draft_incident(llm)

    assert WIKI_LABEL not in llm.prompt(0)
    assert WIKI_LABEL not in llm.prompt(1)


def test_oversized_wiki_context_is_capped_and_the_prompts_still_fit():
    llm = ScriptedLlm(FIRST_PASS, FIRST_PASS)

    report = _draft_incident(llm, wiki_context="Ω" * 200_000)

    assert report.summary == FIRST_PASS["summary"]
    for index in (0, 1):
        assert llm.prompt(index).count("Ω") == 6000
        assert len(llm.prompt(index)) <= 60000


@pytest.mark.parametrize(
    ("wiki_context", "wiki_ref", "kept"),
    [
        ("패턴: 샤드 과다 배치", "샤드 과다 배치", "샤드 과다 배치"),
        ("패턴: 샤드 과다 배치", "위키에 없는 패턴", ""),
        ("", "샤드 과다 배치", ""),
    ],
)
def test_wiki_ref_is_kept_only_when_the_pattern_is_in_the_wiki_context(wiki_context, wiki_ref, kept):
    reply = _with_recommendation(wiki_ref)
    llm = ScriptedLlm(reply, reply)

    report = _draft_incident(llm, wiki_context=wiki_context)

    assert report.recommendations[0].wiki_ref == kept


def test_synthesis_asks_the_wiki_once_with_the_accumulated_symptoms():
    llm = ScriptedLlm(FIRST_PASS, FIRST_PASS, SUMMARY_PASSED)
    wiki = FakeWiki(answer="패턴 A: 마스터 응답 지연")

    report = _synthesize(llm, wiki, _observed())

    assert len(wiki.questions) == 1
    assert MASTER_LINE in wiki.questions[0]
    assert "패턴 A: 마스터 응답 지연" in llm.prompt(0)
    assert report.verification_status is VerificationStatus.PASSED


def test_wiki_failure_does_not_fail_the_synthesis():
    llm = ScriptedLlm(FIRST_PASS, FIRST_PASS, SUMMARY_PASSED)
    wiki = FakeWiki(error=RuntimeError("wiki down"))

    report = _synthesize(llm, wiki, _observed())

    assert len(wiki.questions) == 1
    assert WIKI_LABEL not in llm.prompt(0)
    assert report.verification_status is VerificationStatus.PASSED


def test_synthesis_without_a_wiki_client_asks_nothing():
    llm = ScriptedLlm(FIRST_PASS, FIRST_PASS, SUMMARY_PASSED)

    report = _synthesize(llm, None, _observed())

    assert WIKI_LABEL not in llm.prompt(0)
    assert report.verification_status is VerificationStatus.PASSED


def test_synthesis_without_observed_symptoms_does_not_query_the_wiki():
    llm = ScriptedLlm(FIRST_PASS, FIRST_PASS, SUMMARY_PASSED)
    wiki = FakeWiki(answer="쓰이지 않는 답")

    _synthesize(llm, wiki, Observations(requested=((START, END),)))

    assert wiki.questions == []
    assert WIKI_LABEL not in llm.prompt(0)
