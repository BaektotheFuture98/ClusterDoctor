"""사건 종합의 교정·재수정 단계가 title·future_improvements를 잃지 않는지 검증한다.

LLM은 부르지 않는다. 호출자(call_llm)를 대본대로 답하는 함수로 바꿔 끼운다.
"""

import json
from datetime import datetime

import pytest
from pydantic import ValidationError

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport
from cluster_doctor.incident_analysis_agent.service.report_generation.prompts import (
    build_revision_prompt,
)
from cluster_doctor.incident_analysis_agent.service.report_generation.report_writer import (
    ReportWriter,
)

_DRAFT_MARKER = "--- 교정할 초안 JSON ---\n"
_REPORT_MARKER = "--- 현재 리포트 (JSON) ---\n"

_FIRST_PASS = {
    "title": "쿼리 지연",
    "summary": "19:03에 최대 19.04초 지연이 관측됐다.",
    "summary_evidence_refs": [],
    "findings": [],
    "root_causes": [],
    "recommendations": [],
    "unresolved_questions": [],
    "future_improvements": ["다른 노드의 같은 증상 여부를 확인한다."],
}

_REVIEW_WITHOUT_CARRIED = {
    key: value
    for key, value in _FIRST_PASS.items()
    if key not in ("title", "future_improvements")
}


def _window_report() -> LogAnalysisReport:
    return LogAnalysisReport(
        incident_id="inc-1",
        analyzed_from=datetime(2026, 10, 23, 18, 59, tzinfo=KST),
        analyzed_to=datetime(2026, 10, 23, 19, 5, tzinfo=KST),
    )


class ScriptedLlm:
    """호출 순서대로 JSON을 돌려주고, 받은 메시지를 기록한다."""

    def __init__(self, *replies: dict) -> None:
        self._replies = list(replies)
        self.calls: list[list[dict]] = []

    def __call__(self, messages, *, response_format=None) -> str:
        self.calls.append(messages)
        return json.dumps(self._replies.pop(0), ensure_ascii=False)


def _draft_incident(llm: ScriptedLlm):
    return ReportWriter(call_llm=llm).draft_incident(
        incident_id="inc-1",
        cluster="es",
        window_reports=[_window_report()],
        observations=Observations(),
        evidence=[],
    )


def _json_after(marker: str, messages: list[dict]) -> dict:
    content = messages[-1]["content"]
    return json.loads(content.split(marker, 1)[1].split("\n", 1)[0])


def test_review_input_carries_title_and_future_improvements():
    llm = ScriptedLlm(_FIRST_PASS, _FIRST_PASS)

    _draft_incident(llm)

    review_input = _json_after(_DRAFT_MARKER, llm.calls[1])
    assert review_input["title"] == "쿼리 지연"
    assert review_input["future_improvements"] == ["다른 노드의 같은 증상 여부를 확인한다."]


def test_revision_input_carries_title_and_future_improvements():
    report = _window_report().model_copy(
        update={"title": "쿼리 지연", "future_improvements": ("재발 방지 조치",)}
    )

    prompt = build_revision_prompt(
        report=report, issues=("지적",), evidence=[], incident_final=True
    )

    current = _json_after(_REPORT_MARKER, [{"content": prompt}])
    assert current["title"] == "쿼리 지연"
    assert current["future_improvements"] == ["재발 방지 조치"]


def test_review_reply_without_title_keeps_first_pass_values():
    """교정 응답이 title·future_improvements를 생략해도 1차 종합의 값이 유지된다."""
    llm = ScriptedLlm(_FIRST_PASS, _REVIEW_WITHOUT_CARRIED)

    report = _draft_incident(llm)

    assert report.title == "쿼리 지연"
    assert report.future_improvements == ("다른 노드의 같은 증상 여부를 확인한다.",)


def test_review_reply_missing_a_core_field_still_fails():
    """생략을 메워 주는 것은 title·future_improvements뿐이다. 판단 본문은 그대로 엄격하다."""
    without_summary = {k: v for k, v in _REVIEW_WITHOUT_CARRIED.items() if k != "summary"}
    llm = ScriptedLlm(_FIRST_PASS, without_summary)

    with pytest.raises(ValidationError):
        _draft_incident(llm)


def test_incident_revision_without_title_is_still_applied():
    report = _window_report().model_copy(
        update={"title": "쿼리 지연", "future_improvements": ("재발 방지 조치",)}
    )
    revised_reply = {**_REVIEW_WITHOUT_CARRIED, "summary": "교정된 요약."}
    llm = ScriptedLlm(revised_reply)

    revised = ReportWriter(call_llm=llm).revise_report(
        report, ("지적",), [], incident_final=True
    )

    assert revised is not None
    assert revised.summary == "교정된 요약."
    assert revised.title == "쿼리 지연"
    assert revised.future_improvements == ("재발 방지 조치",)


def test_review_reply_accepts_future_improvements_as_single_string():
    reply = {**_REVIEW_WITHOUT_CARRIED, "title": "쿼리 지연", "future_improvements": "한 항목"}
    llm = ScriptedLlm(_FIRST_PASS, reply)

    report = _draft_incident(llm)

    assert report.future_improvements == ("한 항목",)


def test_review_reply_with_blank_title_keeps_first_pass_title():
    reply = {**_REVIEW_WITHOUT_CARRIED, "title": "  "}
    llm = ScriptedLlm(_FIRST_PASS, reply)

    report = _draft_incident(llm)

    assert report.title == "쿼리 지연"


def test_review_reply_values_win_over_first_pass():
    """이어받기는 비어 있는 값만 채운다. 교정이 새로 낸 값을 되돌리면 안 된다."""
    reply = {**_REVIEW_WITHOUT_CARRIED, "title": "교정된 제목", "future_improvements": ["교정된 개선안"]}
    llm = ScriptedLlm(_FIRST_PASS, reply)

    report = _draft_incident(llm)

    assert report.title == "교정된 제목"
    assert report.future_improvements == ("교정된 개선안",)


def test_review_reply_with_null_future_improvements_is_carried():
    reply = {**_REVIEW_WITHOUT_CARRIED, "title": "쿼리 지연", "future_improvements": None}
    llm = ScriptedLlm(_FIRST_PASS, reply)

    report = _draft_incident(llm)

    assert report.future_improvements == ("다른 노드의 같은 증상 여부를 확인한다.",)


def test_review_reply_with_empty_future_improvements_stays_empty():
    """근거 없는 항목을 모두 뺀 교정 결과는 정당하다. 빈 배열을 생략으로 보지 않는다."""
    reply = {**_REVIEW_WITHOUT_CARRIED, "title": "쿼리 지연", "future_improvements": []}
    llm = ScriptedLlm(_FIRST_PASS, reply)

    report = _draft_incident(llm)

    assert report.future_improvements == ()


def test_review_reply_that_is_not_an_object_still_fails():
    llm = ScriptedLlm(_FIRST_PASS, [])

    with pytest.raises(ValidationError):
        _draft_incident(llm)
