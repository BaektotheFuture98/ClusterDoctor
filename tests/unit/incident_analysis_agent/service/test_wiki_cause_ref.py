import json
from datetime import datetime

import pytest

from cluster_doctor.incident_analysis_agent.agent.wiki_investigation.findings import WikiPattern
from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.time_range import TimeRange
from cluster_doctor.incident_analysis_agent.service.report_generation.schema import (
    DraftReport,
    IncidentDraftReport,
)

WINDOW = TimeRange(datetime(2026, 1, 1, 14, 0, tzinfo=KST), datetime(2026, 1, 1, 14, 10, tzinfo=KST))
PATTERN_ID = "massive-agg-query-timeout"


def _body(cause_extra: dict | None = None, recommendation_extra: dict | None = None) -> str:
    cause = {"cause_type": "root", "statement": "원인", "mechanism": "설명",
             "supporting_evidence_refs": ["E-1"], "counter_evidence_refs": []}
    cause.update(cause_extra or {})
    recommendation = {"text": "조치", "cause_index": 0, "evidence_refs": ["E-1"]}
    recommendation.update(recommendation_extra or {})
    return json.dumps({
        "title": "제목", "summary": "요약", "findings": [], "root_causes": [cause],
        "recommendations": [recommendation], "unresolved_questions": [],
    }, ensure_ascii=False)


def _incident(text: str):
    return IncidentDraftReport.model_validate_json(text).to_domain(
        incident_id="i-1", window=WINDOW, evidence_refs=())


def test_cause_wiki_ref_reaches_the_domain_report():
    report = _incident(_body({"wiki_ref": PATTERN_ID}))
    assert report.root_causes[0].wiki_ref == PATTERN_ID


def test_cause_without_wiki_ref_is_empty():
    assert _incident(_body()).root_causes[0].wiki_ref == ""


@pytest.mark.parametrize("value, expected", [
    (None, ""),
    ([PATTERN_ID, "other"], PATTERN_ID),
    (["", PATTERN_ID], PATTERN_ID),
    ([], ""),
    (7, ""),
    ({"id": PATTERN_ID}, ""),
])
def test_odd_cause_wiki_ref_shapes_do_not_drop_the_draft(value, expected):
    report = _incident(_body({"wiki_ref": value}))
    assert report.root_causes[0].wiki_ref == expected
    assert report.summary == "요약"


@pytest.mark.parametrize("value, expected", [
    (None, ""),
    ([PATTERN_ID, "other"], PATTERN_ID),
    (7, ""),
])
def test_odd_recommendation_wiki_ref_shapes_do_not_drop_the_draft(value, expected):
    report = _incident(_body(recommendation_extra={"wiki_ref": value}))
    assert report.recommendations[0].wiki_ref == expected
    assert report.summary == "요약"


def test_window_draft_has_no_cause_wiki_ref():
    draft = DraftReport.model_validate_json(_body({"wiki_ref": PATTERN_ID}))
    report = draft.to_domain(incident_id="i-1", window=WINDOW, evidence_refs=())
    assert report.root_causes[0].wiki_ref == ""
    assert "wiki_ref" not in DraftReport.model_json_schema()["$defs"]["DraftCause"]["properties"]


def test_wiki_pattern_title_is_the_first_h1():
    page = "---\nid: p\n---\n# 대량 쿼리로 인한 타임아웃\n\n## 원인\n내용"
    assert WikiPattern("p", "이유", page).title == "대량 쿼리로 인한 타임아웃"


def test_wiki_pattern_title_falls_back_to_the_id():
    assert WikiPattern("p", "이유", "## 원인\n내용").title == "p"
