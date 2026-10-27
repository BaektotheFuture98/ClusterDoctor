import inspect

from cluster_doctor.incident_analysis_agent.service.report_generation import prompts
from cluster_doctor.incident_analysis_agent.service.report_generation.report_writer import ReportWriter


def test_analysis_prompt_has_no_wiki_parameter_or_section():
    assert "wiki_context" not in inspect.signature(prompts.build_analysis_prompt).parameters
    assert "wiki_context" not in inspect.signature(ReportWriter.draft_report).parameters
    prompt = prompts.build_analysis_prompt(
        cluster="c", window_label="w", analysis_goal="", evidence=[], observation_summary="s",
    )
    assert "장애 패턴 위키" not in prompt


def test_analysis_rules_do_not_mention_wiki_ref():
    assert "wiki_ref" not in prompts._ANALYSIS_RULES
