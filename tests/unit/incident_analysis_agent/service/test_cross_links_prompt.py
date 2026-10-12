from cluster_doctor.incident_analysis_agent.service.report_generation import prompts

_PROMPTS = (prompts._ANALYSIS_RULES, prompts._INCIDENT_SYNTHESIS_INSTRUCTIONS, prompts._INCIDENT_REVIEW_INSTRUCTIONS)


def test_every_report_prompt_explains_cross_links():
    assert all(prompts._CROSS_LINK_RULES in text for text in _PROMPTS)


def test_prompts_no_longer_ask_for_confidence_or_uncertainties():
    assert not any("confidence" in text or "uncertainties" in text or "확신도" in text for text in _PROMPTS)


def test_actions_are_one_line():
    assert all("한 줄" in text for text in _PROMPTS)
