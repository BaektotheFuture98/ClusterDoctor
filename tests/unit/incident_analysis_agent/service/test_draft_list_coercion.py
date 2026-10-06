import json

import pytest

from cluster_doctor.incident_analysis_agent.service.report_generation.schema import (
    DraftCause,
    DraftFinding,
    DraftRecommendation,
    DraftReport,
    DraftTimelineEntry,
    IncidentDraftReport,
    parse_draft,
)

# 사건 종합 호출은 provider에 타입을 강제하지 않는 느슨한 response_format을 쓰므로
# 배열이어야 할 필드에 문자열이 올 수 있다.
INCIDENT = {
    "summary": "요약",
    "findings": [],
    "root_causes": [
        {
            "statement": "원인",
            "confidence": "Low",
            "uncertainties": "요청이 집중된 타이밍과 힙 상승의 상관관계를 확정할 수 없습니다.",
        }
    ],
    "recommendations": [],
    "unresolved_questions": [],
}


def test_incident_draft_accepts_string_for_uncertainties():
    draft = IncidentDraftReport.model_validate_json(json.dumps(INCIDENT, ensure_ascii=False))

    assert draft.root_causes[0].uncertainties == [
        "요청이 집중된 타이밍과 힙 상승의 상관관계를 확정할 수 없습니다."
    ]


@pytest.mark.parametrize(
    "model, field",
    [
        (DraftTimelineEntry, "evidence_refs"),
        (DraftFinding, "evidence_refs"),
        (DraftCause, "uncertainties"),
        (DraftCause, "supporting_evidence_refs"),
        (DraftCause, "counter_evidence_refs"),
        (DraftRecommendation, "evidence_refs"),
        (DraftReport, "summary_evidence_refs"),
        (DraftReport, "unresolved_questions"),
    ],
)
class TestStringListFields:
    def test_string_becomes_single_item_list(self, model, field):
        assert getattr(model.model_validate({field: "  E-one  "}), field) == ["E-one"]

    def test_string_is_not_split_on_commas_or_newlines(self, model, field):
        value = "a, b\nc"

        assert getattr(model.model_validate({field: value}), field) == [value]

    @pytest.mark.parametrize("empty", [None, "", "   "])
    def test_missing_values_become_empty_list(self, model, field, empty):
        assert getattr(model.model_validate({field: empty}), field) == []

    def test_list_is_unchanged(self, model, field):
        assert getattr(model.model_validate({field: ["E-one", "E-two"]}), field) == [
            "E-one",
            "E-two",
        ]


def test_window_draft_keeps_content_when_a_list_field_is_a_string():
    text = json.dumps(
        {
            "summary": "구간 요약",
            "unresolved_questions": "추가 로그가 필요합니다.",
            "root_causes": [{"statement": "원인", "uncertainties": "확인 불가"}],
        },
        ensure_ascii=False,
    )

    draft = parse_draft(text)

    assert draft.summary == "구간 요약"
    assert draft.unresolved_questions == ["추가 로그가 필요합니다."]
    assert draft.root_causes[0].uncertainties == ["확인 불가"]


def test_missing_required_incident_field_is_still_an_error():
    payload = {key: value for key, value in INCIDENT.items() if key != "findings"}

    with pytest.raises(Exception):
        IncidentDraftReport.model_validate_json(json.dumps(payload, ensure_ascii=False))
