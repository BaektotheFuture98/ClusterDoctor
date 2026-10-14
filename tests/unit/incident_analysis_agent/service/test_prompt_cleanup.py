from cluster_doctor.incident_analysis_agent.agent.prompts.subagent_prompt import (
    _SYSTEM_PROMPT as SUBAGENT_PROMPT,
)
from cluster_doctor.incident_analysis_agent.service.report_generation import prompts as p
from cluster_doctor.incident_orchestrator_agent.agent.prompts.system_prompt import (
    SYSTEM_PROMPT,
)


def test_removed_phrases_are_gone():
    assert "설득" not in SYSTEM_PROMPT and "우회" not in SYSTEM_PROMPT
    assert "차집합" not in SYSTEM_PROMPT
    assert "붉은 배너" not in SYSTEM_PROMPT
    assert "after_agent middleware" not in SUBAGENT_PROMPT
    for text in (p._ANALYSIS_RULES, p._INCIDENT_SYNTHESIS_INSTRUCTIONS,
                 p._INCIDENT_REVIEW_INSTRUCTIONS):
        assert "40자" not in text


def test_kept_phrase_in_orchestrator():
    assert "문장에 다른 시각을 적어도 그 구간이 분석되지는 않는다" in SYSTEM_PROMPT


def test_new_phrases_present():
    assert "60자 내외" in p._ANALYSIS_RULES
    assert "60자 내외" in p._INCIDENT_SYNTHESIS_INSTRUCTIONS
    assert "verdict가 cluster_wide가 아니면" in p._INCIDENT_REVIEW_INSTRUCTIONS
    assert "node_metric_thresholds" in p._ANALYSIS_RULES
    assert "slow_threshold_seconds" in p._ANALYSIS_RULES
    assert "GC 여부를 확인하지 못했다면 unresolved_questions" in p._ANALYSIS_RULES
    assert p._ANALYSIS_RULES.count("GC 여부를 확인하지 못했다면") == 1
    assert "1~2문장" in p._INCIDENT_SYNTHESIS_INSTRUCTIONS
    assert "1~2문장" in p._INCIDENT_REVIEW_INSTRUCTIONS


def test_cause_target_rules_still_injected():
    assert p._CAUSE_TARGET_RULES in p._INCIDENT_SYNTHESIS_INSTRUCTIONS
    assert p._CAUSE_TARGET_RULES in p._INCIDENT_REVIEW_INSTRUCTIONS
