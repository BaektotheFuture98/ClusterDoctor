import json
from datetime import UTC, datetime

from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisReport,
    ReportFinding,
    ReportRecommendation,
    RootCause,
)
from cluster_doctor.incident_analysis_agent.service.report_generation.prompts import (
    build_revision_prompt,
)


def test_revision_includes_complete_editable_report():
    at = datetime(2026, 10, 1, tzinfo=UTC)
    report = LogAnalysisReport(
        incident_id="I",
        analyzed_from=at,
        analyzed_to=at,
        summary="summary",
        findings=(ReportFinding(title="finding", detail="detail to preserve"),),
        root_causes=(
            RootCause(statement="cause", counter_evidence_refs=("E-counter",)),
        ),
        recommendations=(ReportRecommendation(text="action to preserve"),),
        unresolved_questions=("question to preserve",),
    )
    prompt = build_revision_prompt(
        report=report, issues=("fix confidence",), evidence=[]
    )
    assert "detail to preserve" in prompt
    assert "E-counter" in prompt
    assert "action to preserve" in prompt
    payload = prompt.split("--- 현재 리포트 (JSON) ---\n", 1)[1].split("\n\n", 1)[0]
    assert json.loads(payload)["recommendations"] == [{"text": "action to preserve", "evidence_refs": []}]


def test_revision_keeps_summary_and_action_refs_and_context():
    at = datetime(2026, 10, 1, tzinfo=UTC)
    report = LogAnalysisReport(incident_id='I', analyzed_from=at, analyzed_to=at,
        summary='summary', summary_evidence_refs=('E1',),
        recommendations=({'text': 'action', 'evidence_refs': ('E1',)},))
    prompt = build_revision_prompt(report=report, issues=('fix',), evidence=[], analysis_context='{"query_execution_count":2}')
    payload = json.loads(prompt.split('--- 현재 리포트 (JSON) ---\n')[1].split('\n\n')[0])
    assert payload['summary_evidence_refs'] == ['E1']
    assert payload['recommendations'][0]['evidence_refs'] == ['E1']
    assert '{"query_execution_count":2}' in prompt


def test_summary_prompt_separates_observation_from_causal_hypotheses():
    from cluster_doctor.incident_analysis_agent.service.report_generation.prompts import _ANALYSIS_RULES
    assert 'summary에는 관측 사실만' in _ANALYSIS_RULES
    assert 'JVM 사용률만으로 쿼리 지연' in _ANALYSIS_RULES
