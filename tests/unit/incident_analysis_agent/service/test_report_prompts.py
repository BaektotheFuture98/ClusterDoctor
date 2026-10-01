import json
from datetime import UTC, datetime

from cluster_doctor.incident_analysis_agent.model.report import (
    LogAnalysisReport,
    ReportFinding,
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
        recommendations=("action to preserve",),
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
