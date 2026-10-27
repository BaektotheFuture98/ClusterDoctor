"""wiki_ref가 null이어도 초안 전체가 버려지지 않고, 교정 지시가 그 형식을 밝힌다."""

import json

from cluster_doctor.incident_analysis_agent.service.report_generation.prompts import (
    _INCIDENT_REVIEW_INSTRUCTIONS,
)
from cluster_doctor.incident_analysis_agent.service.report_generation.schema import (
    DraftReport,
    IncidentDraftReport,
    parse_draft,
)

INCIDENT = {"title": "t", "summary": "s", "findings": [], "root_causes": [], "unresolved_questions": []}


def _recommendation(wiki_ref) -> dict:
    return {"text": "조치", "wiki_ref": wiki_ref}


def test_draft_report_reads_null_wiki_ref_as_empty():
    draft = DraftReport.model_validate({"summary": "구간 요약", "recommendations": [_recommendation(None)]})

    assert draft.recommendations[0].wiki_ref == ""


def test_parse_draft_keeps_the_draft_when_wiki_ref_is_null():
    text = json.dumps({"summary": "구간 요약", "recommendations": [_recommendation(None)]}, ensure_ascii=False)

    assert parse_draft(text).summary == "구간 요약"


def test_incident_draft_reads_null_wiki_ref_as_empty():
    draft = IncidentDraftReport.model_validate({**INCIDENT, "recommendations": [_recommendation(None)]})

    assert draft.recommendations[0].wiki_ref == ""


def test_string_wiki_ref_is_left_as_written():
    draft = IncidentDraftReport.model_validate({**INCIDENT, "recommendations": [_recommendation(" pat-a ")]})

    assert draft.recommendations[0].wiki_ref == " pat-a "


def test_review_instructions_say_an_empty_wiki_ref_is_an_empty_string_not_null():
    assert "null이 아니라 빈 문자열" in _INCIDENT_REVIEW_INSTRUCTIONS
