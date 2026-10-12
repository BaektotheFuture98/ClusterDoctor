from cluster_doctor.incident_analysis_agent.model.report import RootCause
from cluster_doctor.incident_analysis_agent.service.report_generation.schema import DraftCause
from cluster_doctor.incident_orchestrator_agent.model.incident_report import CauseAssessment


def test_cause_models_have_no_confidence_or_uncertainties():
    for fields in (DraftCause.model_fields, RootCause.model_fields, CauseAssessment.__dataclass_fields__):
        assert "confidence" not in fields
        assert "uncertainties" not in fields


def test_legacy_payload_is_still_readable():
    cause = RootCause.model_validate({"statement": "s", "confidence": "Low", "uncertainties": ["x"]})
    assert cause.statement == "s"
