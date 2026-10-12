from cluster_doctor.incident_analysis_agent.service.report_generation import prompts

_PROMPTS = (prompts._ANALYSIS_RULES, prompts._INCIDENT_SYNTHESIS_INSTRUCTIONS, prompts._INCIDENT_REVIEW_INSTRUCTIONS)


def test_every_report_prompt_explains_cross_links():
    assert all(prompts._CROSS_LINK_RULES in text for text in _PROMPTS)


def test_prompts_no_longer_ask_for_confidence_or_uncertainties():
    assert not any("confidence" in text or "uncertainties" in text or "확신도" in text for text in _PROMPTS)


def test_actions_are_one_line():
    assert all("한 줄" in text for text in _PROMPTS)


def test_prompts_have_no_leftover_check_procedure_wording():
    banned = ("확인 절차", "이스케이프한 두 개의 개행", "가설/가능성")
    assert not any(phrase in text for text in _PROMPTS for phrase in banned)
    assert "확인 절차" not in prompts._REVISION_HEADER


def test_schema_descriptions_ask_for_one_line_actions():
    from cluster_doctor.incident_analysis_agent.service.report_generation.schema import DraftRecommendation, DraftReport

    assert "한 줄" in DraftRecommendation.model_fields["text"].description
    assert "한 줄" in DraftReport.model_fields["recommendations"].description
