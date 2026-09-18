
from cluster_doctor.application.port.outbound.diagnosis_analyzer import (
    DiagnosisAnalyzer,
    LlmApiError,
    LlmResponseError,
)


def test_errors_are_runtime_errors():
    assert issubclass(LlmApiError, RuntimeError)
    assert issubclass(LlmResponseError, RuntimeError)


def test_errors_are_distinct_types():
    assert not issubclass(LlmApiError, LlmResponseError)
    assert not issubclass(LlmResponseError, LlmApiError)


def test_port_still_declares_analyze():
    assert hasattr(DiagnosisAnalyzer, "analyze")
    assert getattr(DiagnosisAnalyzer.analyze, "__isabstractmethod__", False) is True
