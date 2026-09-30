"""Regression tests for owner-based model placement."""


def test_shared_evidence_model_is_exposed_without_a_basemodel_package():
    from cluster_doctor.incident_analysis_agent.model.evidence import Evidence, EvidenceSource

    assert Evidence.__name__ == "Evidence"
    assert EvidenceSource.SLOWLOG.value == "slowlog"


def test_minute_workflow_keeps_state_models_and_llm_schemas_separate():
    from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.model import RawRecord
    from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.schema import MapOutput
    from cluster_doctor.incident_analysis_agent.workflow.minute_analysis.state import (
        MinuteAnalysisState,
    )

    assert RawRecord.__name__ == "RawRecord"
    assert MapOutput(selected=[]).selected == []
    assert MinuteAnalysisState.__name__ == "MinuteAnalysisState"
