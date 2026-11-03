from datetime import datetime

import pytest

from cluster_doctor.incident_analysis_agent.model.kst import KST
from cluster_doctor.incident_analysis_agent.model.observations import Observations
from cluster_doctor.incident_analysis_agent.model.report import LogAnalysisReport
from cluster_doctor.incident_analysis_agent.service.report_generation import prompts
from cluster_doctor.incident_analysis_agent.service.report_generation.prompts import (
    _INCIDENT_REVIEW_INSTRUCTIONS,
    _INCIDENT_REVISION_INSTRUCTIONS,
    _WIKI_FINDINGS_SECTION_TITLE,
    _insert_before,
    build_incident_review_messages,
    build_incident_synthesis_messages,
)
from cluster_doctor.incident_analysis_agent.service.report_generation.report_writer import ReportWriter

AT = datetime(2026, 1, 1, 14, 0, tzinfo=KST)
CROSS_LINK_MARKER = "cross_links는 코드가 계산한 시각·노드 대조와 판정이다"
CAUSE_TARGET_MARKER = "원인의 속성과 대상(slow_query_profile, slow_concentration)"


def _synthesis_system() -> str:
    return build_incident_synthesis_messages(
        cluster="c", analyzed_from=AT, analyzed_to=AT,
        previous_windows_json="{}", analysis_context="{}")[0]["content"]


def _review_system() -> str:
    return build_incident_review_messages(
        analyzed_from=AT, analyzed_to=AT, draft_json="{}", analysis_context="{}")[0]["content"]


def test_synthesis_treats_wiki_as_knowledge_basis_not_observation():
    system = _synthesis_system()
    assert "지식 근거" in system
    assert "증상 일치만으로는 원인을 확정하지 않으므로" in system
    assert "root_causes의 wiki_ref" in system
    assert "관측과 위키가 어긋나면 관측을 따른다" in system
    assert "위키 내용은 근거가 아니므로" not in system


def test_synthesis_contract_lists_the_cause_wiki_ref():
    assert 'counter_evidence_refs, wiki_ref}' in _synthesis_system()


def test_review_instructions_check_wiki_against_pages():
    assert "지식 근거" in _INCIDENT_REVIEW_INSTRUCTIONS
    assert "supporting_evidence_refs에 그 패턴의 증상과 맞는 관측이 있을 때만 둔다" in _INCIDENT_REVIEW_INSTRUCTIONS
    assert "이 호출에는 위키 조사 결과가 없어" not in _INCIDENT_REVIEW_INSTRUCTIONS
    assert _review_system() == _INCIDENT_REVIEW_INSTRUCTIONS


def test_revision_instructions_do_not_ask_to_verify_wiki_pages():
    assert "이 호출에는 위키 조사 결과가 없어 위키 대조는 하지 않는다" in _INCIDENT_REVISION_INSTRUCTIONS
    assert "초안의 wiki_ref는 그대로 둔다" in _INCIDENT_REVISION_INSTRUCTIONS
    assert "supporting_evidence_refs에 그 패턴의 증상과 맞는 관측이 있을 때만 둔다" not in _INCIDENT_REVISION_INSTRUCTIONS


@pytest.mark.parametrize("text", [_INCIDENT_REVIEW_INSTRUCTIONS, _INCIDENT_REVISION_INSTRUCTIONS])
def test_review_and_revision_keep_the_shared_rules(text):
    assert "@@WIKI_RULE@@" not in text
    assert CROSS_LINK_MARKER in text
    assert CAUSE_TARGET_MARKER in text
    assert "root_causes와 recommendations의 wiki_ref는 문자열 하나다" in text


def test_synthesis_and_window_prompts_keep_the_cross_link_rules():
    assert CROSS_LINK_MARKER in _synthesis_system()
    assert CAUSE_TARGET_MARKER in _synthesis_system()
    assert CROSS_LINK_MARKER in prompts._ANALYSIS_RULES


def test_wiki_block_title_names_the_role():
    assert "지식 근거" in _WIKI_FINDINGS_SECTION_TITLE
    assert "이번 사건의 관측이 아니다" in _WIKI_FINDINGS_SECTION_TITLE


def test_insert_before_requires_exactly_one_anchor():
    assert _insert_before("가\n나", "\n나", "추가") == "가추가\n나"
    with pytest.raises(ValueError):
        _insert_before("가나", "\n다", "추가")
    with pytest.raises(ValueError):
        _insert_before("\n다\n다", "\n다", "추가")


def test_incident_final_revision_uses_the_revision_instructions():
    sent = []

    def call_llm(messages, **kwargs):
        sent.append(messages)
        return "{}"

    report = LogAnalysisReport(incident_id="i-1", analyzed_from=AT, analyzed_to=AT, summary="요약")
    writer = ReportWriter(call_llm=call_llm)
    assert writer.revise_report(report, ("지적",), [], observations=Observations(),
                                incident_final=True) is None
    assert sent[0][0] == {"role": "system", "content": _INCIDENT_REVISION_INSTRUCTIONS}
