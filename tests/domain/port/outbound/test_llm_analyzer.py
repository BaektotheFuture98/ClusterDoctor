import pytest

from cluster_doctor.domain.port.outbound.llm_analyzer import (
    LlmAnalyzer,
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
    assert hasattr(LlmAnalyzer, "analyze")
    assert getattr(LlmAnalyzer.analyze, "__isabstractmethod__", False) is True
